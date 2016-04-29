package org.commons.async;

/**
 * @author ahmad
 */
public final class SimpleExecutionResult<T> implements AsyncResult<T>{

    private final T result;
    private final Throwable cause;
    private final boolean succeeded;

    public SimpleExecutionResult(T result) {
        this(result, null, true);
    }

    public SimpleExecutionResult(Throwable cause) {
        this(null, cause, false);
    }

    public SimpleExecutionResult(boolean succeeded) {
        this(null, null, succeeded);
    }

    public SimpleExecutionResult(T result, Throwable cause, boolean succeeded) {
        this.result = result;
        this.cause = cause;
        this.succeeded = succeeded;
    }

    @Override
    public T result() {
        return result;
    }

    @Override
    public Throwable cause() {
        return cause;
    }

    @Override
    public boolean succeeded() {
        return succeeded;
    }

    @Override
    public boolean failed() {
        return cause != null;
    }

    @Override
    public String toString() {
        return "{" +
                "result=" + result +
                ", cause=" + cause +
                ", succeeded=" + succeeded +
                '}';
    }

}