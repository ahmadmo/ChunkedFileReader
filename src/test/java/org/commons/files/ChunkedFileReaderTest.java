package org.commons.files;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/**
 * @author ahmad
 */
public class ChunkedFileReaderTest {

    private static final Path PATH = Paths.get("path/to/large_file");

    private long expectedFileSize;
    private long fileSize;

    @Before
    public void setup() throws IOException {
        expectedFileSize = Files.size(PATH);
    }

    @Test
    public void doIt() {
        ChunkedFileReader.path(PATH)
                .bufferHandler(event -> {
                    if (event.succeeded()) {
                        fileSize += event.result().length;
                    } else {
                        System.err.println("FAILED TO READ FROM FILE. due to : " + event.cause().getMessage());
                    }
                }).end();

        assertEquals(expectedFileSize, fileSize);
    }

}
