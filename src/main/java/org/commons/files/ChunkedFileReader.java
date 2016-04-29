package org.commons.files;

import org.commons.async.AsyncResult;
import org.commons.async.Handler;
import org.commons.async.SimpleExecutionResult;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author ahmad
 */
public final class ChunkedFileReader {

    private static final int DEFAULT_CHUNK_SIZE = Integer.MAX_VALUE;
    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private static final byte[] EOF = {-1};

    private final Path path;
    private final AtomicBoolean opened = new AtomicBoolean();

    private FileChannel inChannel = null;
    private FileLock fileLock = null;

    private long fileSize;
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    private int bufferSize = DEFAULT_BUFFER_SIZE;

    private AsyncResult<Void> openResult;
    private Handler<AsyncResult<Void>> openHandler;
    private AsyncResult<Void> closeResult;
    private Handler<AsyncResult<Void>> closeHandler;

    private Handler<AsyncResult<byte[]>> bufferHandler;

    private ChunkedFileReader(Path path) {
        this.path = path;
    }

    public ChunkedFileReader openHandler(Handler<AsyncResult<Void>> openHandler) {
        if (openResult != null) {
            openHandler.handle(openResult);
        }
        this.openHandler = openHandler;
        return this;
    }

    public ChunkedFileReader chunkSize(int chunkSize) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("chunkSize must be a positive integer");
        }
        this.chunkSize = chunkSize;
        return this;
    }

    public ChunkedFileReader bufferHandler(Handler<AsyncResult<byte[]>> bufferHandler) {
        if (opened.get()) {
            throw new IllegalStateException("You cannot set an handler for the buffer after opening");
        }
        this.bufferHandler = Objects.requireNonNull(bufferHandler);
        return this;
    }

    public ChunkedFileReader bufferSize(int bufferSize) {
        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must be a positive integer");
        }
        this.bufferSize = bufferSize;
        return this;
    }

    public ChunkedFileReader closeHandler(Handler<AsyncResult<Void>> closeHandler) {
        if (closeResult != null) {
            closeHandler.handle(closeResult);
        }
        this.closeHandler = closeHandler;
        return this;
    }

    private void openChannel() {
        try {
            inChannel = FileChannel.open(path, StandardOpenOption.READ);
            fileLock = inChannel.tryLock(0, Long.MAX_VALUE, true);
            fileSize = inChannel.size();

            openResult = new SimpleExecutionResult<>(true);
        } catch (IOException e) {
            openResult = new SimpleExecutionResult<>(e);
        }

        if (openHandler != null) {
            openHandler.handle(openResult);
        }
    }

    private void readChannel() {
        // setting up buffer handler thread
        final BlockingQueue<byte[]> bufferQueue = new LinkedBlockingQueue<>();
        final CountDownLatch closeLatch = new CountDownLatch(1);
        ExecutorService bufferHandlerThread = Executors.newSingleThreadExecutor();

        bufferHandlerThread.execute(() -> {
            byte[] bytes;

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    bytes = bufferQueue.take();
                    if (bytes == null || bytes == EOF) {
                        break;
                    }
                    bufferHandler.handle(new SimpleExecutionResult<>(bytes));
                } catch (InterruptedException ignored) {
                }
            }

            closeLatch.countDown();
        });
        bufferHandlerThread.shutdown();

        // start reading from channel
        long position = 0;
        long mapSize;

        byte[] bytes = null;
        int offset = 0, length;

        try {
            while ((mapSize = Math.min(chunkSize, fileSize - position)) > 0) {
                MappedByteBuffer mbb = inChannel.map(FileChannel.MapMode.READ_ONLY, position, mapSize);

                while (mbb.hasRemaining()) {

                    if (bytes == null) {
                        bytes = new byte[Math.min(mbb.remaining(), bufferSize)];
                        offset = 0;
                    }

                    length = bytes.length - offset;
                    mbb.get(bytes, offset, length);
                    offset += length;

                    if (offset == bytes.length) {
                        bufferQueue.offer(bytes);
                        bytes = null;
                    }

                }

                position += mapSize;
            }
        } catch (IOException e) {
            bufferHandler.handle(new SimpleExecutionResult<>(e));
        }

        // signal end of file
        bufferQueue.offer(EOF);

        try {
            closeLatch.await();
        } catch (InterruptedException ignored) {
        }
    }

    private void closeChannel() {
        if (fileLock != null) {
            try {
                fileLock.release();
            } catch (IOException ignored) {
            }
        }

        if (inChannel != null) {
            try {
                inChannel.close();
                closeResult = new SimpleExecutionResult<>(true);
            } catch (IOException e) {
                closeResult = new SimpleExecutionResult<>(e);
            }

            if (closeHandler != null) {
                closeHandler.handle(closeResult);
            }
        }
    }

    public void end() {
        if (bufferHandler == null) {
            throw new IllegalStateException("You must set an handler for the buffer before opening");
        }

        if (!opened.compareAndSet(false, true)) {
            throw new IllegalStateException("You cannot open the channel twice");
        }

        openChannel();

        if (openResult.succeeded()) {
            try {
                readChannel();
            } finally {
                closeChannel();
            }
        }
    }

    public static ChunkedFileReader path(Path path) {
        return new ChunkedFileReader(path);
    }

}
