package org.commons.async;

/**
 * @author ahmad
 */
@FunctionalInterface
public interface Handler<E> {

    void handle(E event);

}
