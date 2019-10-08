package org.elasticsearch.common;

import java.util.concurrent.atomic.AtomicReference;

public class SetOnce<T> {

    private final class Wrapper {
        private T object;

        private Wrapper(T object) {
            this.object = object;
        }
    }

    public static final class AlreadySetException extends IllegalStateException {
        private AlreadySetException() {
            super("The object cannot be set twice!");
        }
    }

    private final AtomicReference<Wrapper> ref;

    public SetOnce(T object) {
        ref = new AtomicReference<>(new Wrapper(object));
    }

    public SetOnce() {
        ref = new AtomicReference<>();
    }

    public boolean trySet(T object) {
        return ref.compareAndSet(null, new Wrapper(object));
    }

    public void set(T object) {
        if (trySet(object) == false) {
            throw new AlreadySetException();
        }
    }

    public T get() {
        Wrapper wrapper = ref.get();
        return wrapper == null ? null : wrapper.object;
    }
}
