package org.elasticsearch.nio.utils;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class CompletableContext<T> {

    private final CompletableFuture<T> completableFuture = new CompletableFuture<>();

    public void addListener(BiConsumer<T, ? super Exception> listener) {
        BiConsumer<T, Throwable> castThrowable = (v, t) -> {
            if (t == null) {
                listener.accept(v, null);
            } else {
                assert !(t instanceof Error) : "Cannot be error";
                listener.accept(v, (Exception) t);
            }
        };
        completableFuture.whenComplete(castThrowable);
    }

    public boolean isDone() {
        return completableFuture.isDone();
    }

    public boolean isCompletedExceptionally() {
        return completableFuture.isCompletedExceptionally();
    }

    public boolean completeExceptionally(Exception ex) {
        return completableFuture.completeExceptionally(ex);
    }

    public boolean complete(T value) {
        return completableFuture.complete(value);
    }
}
