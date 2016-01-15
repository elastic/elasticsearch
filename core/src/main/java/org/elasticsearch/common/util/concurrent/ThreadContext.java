/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.util.concurrent;

import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A ThreadContext is a map of string headers and a transient map of keyed objects that are associated with
 * a thread. It allows to store and retrieve header information across method calls, network calls as well as threads spawned from a
 * thread that has a {@link ThreadContext} associated with. Threads spawned from a {@link org.elasticsearch.threadpool.ThreadPool} have out of the box
 * support for {@link ThreadContext} and all threads spawned will inherit the {@link ThreadContext} from the thread that it is forking from.".
 * Network calls will also preserve the senders headers automatically.
 * <p>
 * Consumers of ThreadContext usually don't need to interact with adding or stashing contexts. Every elasticsearch thread is managed by a thread pool or executor
 * being responsible for stashing and restoring the threads context. For instance if a network request is received, all headers are deserialized from the network
 * and directly added as the headers of the threads {@link ThreadContext} (see {@link #readHeaders(StreamInput)}. In order to not modify the context that is currently
 * active on this thread the network code uses a try/with pattern to stash it's current context, read headers into a fresh one and once the request is handled or a handler thread
 * is forked (which in turn inherits the context) it restores the previous context. For instance:
 *
 * <pre>
 *     // current context is stashed and replaced with a default context
 *     try (StoredContext context = threadContext.stashContext()) {
 *         threadContext.readHeaders(in); // read headers into current context
 *         if (fork) {
 *             threadPool.execute(() -> request.handle()); // inherits context
 *         } else {
 *             request.handle();
 *         }
 *     }
 *     // previous context is restored on StoredContext#close()
 * </pre>
 *
 * </p>
 *
 */
public final class ThreadContext implements Closeable, Writeable<ThreadContext.ThreadContextStruct>{

    public static final String PREFIX = "request.headers";
    private final ThreadContextStruct defaultContext;
    private final ContextThreadLocal threadLocal;

    /**
     * Creates a new ThreadContext instance
     * @param settings the settings to read the default request headers from
     */
    public ThreadContext(Settings settings) {
        Settings headers = settings.getAsSettings(PREFIX);
        if (headers == null) {
            this.defaultContext = new ThreadContextStruct(Collections.emptyMap());
        } else {
            Map<String, String> defaultHeader = new HashMap<>();
            for (String key : headers.names()) {
                defaultHeader.put(key, headers.get(key));
            }
            this.defaultContext = new ThreadContextStruct(defaultHeader);
        }
        threadLocal = new ContextThreadLocal(defaultContext);
    }

    @Override
    public void close() throws IOException {
        threadLocal.close();
    }

    /**
     * Removes the current context and resets a default context. The removed context can be
     * restored when closing the returned {@link StoredContext}
     */
    public StoredContext stashContext() {
        final ThreadContextStruct context = threadLocal.get();
        threadLocal.set(null);
        return () -> {
            threadLocal.set(context);
        };
    }

    /**
     * Just like {@link #stashContext()} but no default context is set.
     */
    public StoredContext newStoredContext() {
        final ThreadContextStruct context = threadLocal.get();
        return () -> {
            threadLocal.set(context);
        };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        threadLocal.get().writeTo(out);
    }

    @Override
    public ThreadContextStruct readFrom(StreamInput in) throws IOException {
        return defaultContext.readFrom(in);
    }

    /**
     * Reads the headers from the stream into the current context
     */
    public void readHeaders(StreamInput in) throws IOException {
        threadLocal.set(readFrom(in));
    }


    /**
     * Returns the header for the given key or <code>null</code> if not present
     */
    public String getHeader(String key) {
        return threadLocal.get().headers.get(key);
    }

    /**
     * Returns all of the current contexts headers
     */
    public Map<String, String> getHeaders() {
        return threadLocal.get().headers;
    }

    /**
     * Copies all header key, value pairs into the current context
     */
    public void copyHeaders(Iterable<Map.Entry<String, String>> headers) {
        threadLocal.set(threadLocal.get().copyHeaders(headers));
    }

    /**
     * Puts a header into the context
     */
    public void putHeader(String key, String value) {
        putHeader(Collections.singletonMap(key, value));
    }

    /**
     * Puts all of the given headers into this context
     */
    public void putHeader(Map<String, String> header) {
        threadLocal.set(threadLocal.get().putHeaders(header));
    }

    /**
     * Puts a transient header object into this context
     */
    public void putTransient(String key, Object value) {
        threadLocal.set(threadLocal.get().putTransient(key, value));
    }

    /**
     * Returns a transient header object or <code>null</code> if there is no header for the given key
     */
    public <T> T getTransient(String key) {
        return (T) threadLocal.get().transientHeaders.get(key);
    }

    public interface StoredContext extends AutoCloseable {
        @Override
        void close();

        default void restore() {
            close();
        }
    }

    static final class ThreadContextStruct implements Writeable<ThreadContextStruct> {
        private final Map<String,String> headers;
        private final Map<String, Object> transientHeaders;

        private ThreadContextStruct(StreamInput in) throws IOException {
            int numValues = in.readVInt();
            Map<String, String> headers = numValues == 0 ? Collections.emptyMap() : new HashMap<>(numValues);
            for (int i = 0; i < numValues; i++) {
                headers.put(in.readString(), in.readString());
            }
            this.headers = headers;
            this.transientHeaders = Collections.emptyMap();
        }

        private ThreadContextStruct(Map<String, String> headers, Map<String, Object> transientHeaders) {
            this.headers = headers;
            this.transientHeaders = transientHeaders;
        }

        private ThreadContextStruct(Map<String, String> headers) {
            this(headers, Collections.emptyMap());
        }

        private ThreadContextStruct putHeaders(Map<String, String> headers) {
            if (headers.isEmpty()) {
                return this;
            } else {
                Map<String, String> newHeaders = new HashMap<>(this.headers);
                newHeaders.putAll(headers);
                return new ThreadContextStruct(newHeaders, transientHeaders);
            }
        }

        private ThreadContextStruct putTransient(String key, Object value) {
            Map<String, Object> newTransient = new HashMap<>(this.transientHeaders);
            if (newTransient.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException("value for key [" + key + "] already present");
            }
            return new ThreadContextStruct(headers, newTransient);
        }

        boolean isEmpty() {
            return headers.isEmpty() && transientHeaders.isEmpty();
        }


        private ThreadContextStruct copyHeaders(Iterable<Map.Entry<String, String>> headers) {
            Map<String, String> newHeaders = new HashMap<>();
            for (Map.Entry<String, String> header : headers) {
                newHeaders.put(header.getKey(), header.getValue());
            }
            return putHeaders(newHeaders);
        }

        @Override
        public ThreadContextStruct readFrom(StreamInput in) throws IOException {
            return new ThreadContextStruct(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            int keys = headers.size();
            out.writeVInt(keys);
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                out.writeString(entry.getKey());
                out.writeString(entry.getValue());
            }
        }

    }

    private static class ContextThreadLocal extends CloseableThreadLocal<ThreadContextStruct> {
        private final ThreadContextStruct defaultStruct;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private ContextThreadLocal(ThreadContextStruct defaultStruct) {
            this.defaultStruct = defaultStruct;
        }

        @Override
        public void set(ThreadContextStruct object) {
            try {
                if (object == defaultStruct) {
                    super.set(null);
                } else {
                    super.set(object);
                }
            } catch (NullPointerException ex) {
                /* This is odd but CloseableThreadLocal throws a NPE if it was closed but still accessed.
                   to get a real exception we call ensureOpen() to tell the user we are already closed.*/
                ensureOpen();
                throw ex;
            }
        }

        @Override
        public ThreadContextStruct get() {
            try {
                ThreadContextStruct threadContextStruct = super.get();
                if (threadContextStruct != null) {
                    return threadContextStruct;
                }
                return defaultStruct;
            } catch (NullPointerException ex) {
                /* This is odd but CloseableThreadLocal throws a NPE if it was closed but still accessed.
                   to get a real exception we call ensureOpen() to tell the user we are already closed.*/
                ensureOpen();
                throw ex;
            }
        }

        private void ensureOpen() {
            if (closed.get()) {
                throw new IllegalStateException("threadcontext is already closed");
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                super.close();
            }
        }
    }
}
