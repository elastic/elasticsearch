package org.elasticsearch.nio.utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FutureUtils {

    /**
     * Calls {@link Future#get()} without the checked exceptions.
     *
     * @param future to dereference
     * @param <T> the type returned
     * @return the value of the future
     */
    public static <T> T get(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Future got interrupted", e);
        } catch (ExecutionException e) {
            throw rethrowExecutionException(e);
        }
    }

    private static RuntimeException rethrowExecutionException(ExecutionException e) {
        if (e.getCause() instanceof RuntimeException) {
            return (RuntimeException) e.getCause();
        } else {
            return new RuntimeException("Failed execution", e);
        }
    }
}
