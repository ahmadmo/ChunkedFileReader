package org.commons.async;

/**
 * @author ahmad
 */
public interface AsyncResult<T> {

    T result();

    Throwable cause();

    boolean succeeded();

    boolean failed();

}
