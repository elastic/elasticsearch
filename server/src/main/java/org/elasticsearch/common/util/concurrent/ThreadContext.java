/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.tasks.Task;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_COUNT;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_SIZE;
import static org.elasticsearch.tasks.Task.HEADERS_TO_COPY;

/**
 * A ThreadContext is a map of string headers and a transient map of keyed objects that are associated with
 * a thread. It allows to store and retrieve header information across method calls, network calls as well as threads spawned from a
 * thread that has a {@link ThreadContext} associated with. Threads spawned from a {@link org.elasticsearch.threadpool.ThreadPool}
 * have out of the box support for {@link ThreadContext} and all threads spawned will inherit the {@link ThreadContext} from the thread
 * that it is forking from.". Network calls will also preserve the senders headers automatically.
 * <p>
 * Consumers of ThreadContext usually don't need to interact with adding or stashing contexts. Every elasticsearch thread is managed by
 * a thread pool or executor being responsible for stashing and restoring the threads context. For instance if a network request is
 * received, all headers are deserialized from the network and directly added as the headers of the threads {@link ThreadContext}
 * (see {@link #readHeaders(StreamInput)}. In order to not modify the context that is currently active on this thread the network code
 * uses a try/with pattern to stash it's current context, read headers into a fresh one and once the request is handled or a handler thread
 * is forked (which in turn inherits the context) it restores the previous context. For instance:
 * </p>
 * <pre>
 *     // current context is stashed and replaced with a default context
 *     try (StoredContext context = threadContext.stashContext()) {
 *         threadContext.readHeaders(in); // read headers into current context
 *         if (fork) {
 *             threadPool.execute(() -&gt; request.handle()); // inherits context
 *         } else {
 *             request.handle();
 *         }
 *     }
 *     // previous context is restored on StoredContext#close()
 * </pre>
 *
 */
public final class ThreadContext implements Writeable {

    public static final String PREFIX = "request.headers";
    public static final Setting<Settings> DEFAULT_HEADERS_SETTING = Setting.groupSetting(PREFIX + ".", Property.NodeScope);

    /**
     * Name for the {@link #stashWithOrigin origin} attribute.
     */
    public static final String ACTION_ORIGIN_TRANSIENT_NAME = "action.origin";

    private static final Logger logger = LogManager.getLogger(ThreadContext.class);
    private static final ThreadContextStruct DEFAULT_CONTEXT = new ThreadContextStruct();
    private final Map<String, String> defaultHeader;
    private final ThreadLocal<ThreadContextStruct> threadLocal;
    private final int maxWarningHeaderCount;
    private final long maxWarningHeaderSize;

    /**
     * Creates a new ThreadContext instance
     * @param settings the settings to read the default request headers from
     */
    public ThreadContext(Settings settings) {
        this.defaultHeader = buildDefaultHeaders(settings);
        this.threadLocal = ThreadLocal.withInitial(() -> DEFAULT_CONTEXT);
        this.maxWarningHeaderCount = SETTING_HTTP_MAX_WARNING_HEADER_COUNT.get(settings);
        this.maxWarningHeaderSize = SETTING_HTTP_MAX_WARNING_HEADER_SIZE.get(settings).getBytes();
    }

    /**
     * Removes the current context and resets a default context. The removed context can be
     * restored by closing the returned {@link StoredContext}.
     * @return a stored context that will restore the current context to its state at the point this method was called
     */
    public StoredContext stashContext() {
        return stashContextPreservingRequestHeaders(Collections.emptySet());
    }

    /**
     * Just like {@link #stashContext()} but preserves request headers specified via {@code requestHeaders},
     * if these exist in the context before stashing.
     */
    public StoredContext stashContextPreservingRequestHeaders(Set<String> requestHeaders) {
        final ThreadContextStruct context = threadLocal.get();

        /*
         * When the context is stashed, it should be empty, except for headers that were specified to be preserved via `requestHeaders`
         * and a set of default headers such as X-Opaque-ID, which are always copied (specified via `Task.HEADERS_TO_COPY`).
         *
         * X-Opaque-ID should be preserved in a threadContext in order to propagate this across threads.
         * This is needed so the DeprecationLogger in another thread can see the value of X-Opaque-ID provided by a user.
         * The same is applied to Task.TRACE_ID and other values specified in `Task.HEADERS_TO_COPY`.
         */
        final Set<String> requestHeadersToCopy = getRequestHeadersToCopy(requestHeaders);
        boolean hasHeadersToCopy = false;
        if (context.requestHeaders.isEmpty() == false) {
            for (String header : requestHeadersToCopy) {
                if (context.requestHeaders.containsKey(header)) {
                    hasHeadersToCopy = true;
                    break;
                }
            }
        }

        boolean hasTransientHeadersToCopy = context.transientHeaders.containsKey(Task.APM_TRACE_CONTEXT);

        ThreadContextStruct threadContextStruct = DEFAULT_CONTEXT;
        if (hasHeadersToCopy) {
            Map<String, String> copiedHeaders = getHeadersPresentInContext(context, requestHeadersToCopy);
            threadContextStruct = DEFAULT_CONTEXT.putHeaders(copiedHeaders);
        }
        if (hasTransientHeadersToCopy) {
            threadContextStruct = threadContextStruct.putTransient(
                Task.APM_TRACE_CONTEXT,
                context.transientHeaders.get(Task.APM_TRACE_CONTEXT)
            );
        }
        threadLocal.set(threadContextStruct);

        // If the node and thus the threadLocal get closed while this task is still executing, we don't want this runnable to fail with an
        // uncaught exception
        return storedOriginalContext(context);
    }

    public StoredContext stashContextPreservingRequestHeaders(final String... requestHeaders) {
        return stashContextPreservingRequestHeaders(Set.of(requestHeaders));
    }

    /**
     * When using a {@link org.elasticsearch.tracing.Tracer} to capture activity in Elasticsearch, when a parent span is already
     * in progress, it is necessary to start a new context before beginning a child span. This method creates a context,
     * moving tracing-related fields to different names so that a new child span can be started. This child span will pick up
     * the moved fields and use them to establish the parent-child relationship.
     *
     * @return a stored context, which can be restored when this context is no longer needed.
     */
    public StoredContext newTraceContext() {
        final ThreadContextStruct originalContext = threadLocal.get();
        final Map<String, String> newRequestHeaders = new HashMap<>(originalContext.requestHeaders);
        final Map<String, Object> newTransientHeaders = new HashMap<>(originalContext.transientHeaders);

        final String previousTraceParent = newRequestHeaders.remove(Task.TRACE_PARENT_HTTP_HEADER);
        if (previousTraceParent != null) {
            newTransientHeaders.put("parent_" + Task.TRACE_PARENT_HTTP_HEADER, previousTraceParent);
        }

        final String previousTraceState = newRequestHeaders.remove(Task.TRACE_STATE);
        if (previousTraceState != null) {
            newTransientHeaders.put("parent_" + Task.TRACE_STATE, previousTraceState);
        }

        final Object previousTraceContext = newTransientHeaders.remove(Task.APM_TRACE_CONTEXT);
        if (previousTraceContext != null) {
            newTransientHeaders.put("parent_" + Task.APM_TRACE_CONTEXT, previousTraceContext);
        }

        // this is the context when this method returns
        final ThreadContextStruct newContext = new ThreadContextStruct(
            newRequestHeaders,
            originalContext.responseHeaders,
            newTransientHeaders,
            originalContext.isSystemContext,
            originalContext.warningHeadersSize
        );
        threadLocal.set(newContext);
        // Tracing shouldn't interrupt the propagation of response headers, so in the same as
        // #newStoredContextPreservingResponseHeaders(), pass on any potential changes to the response headers.
        return () -> {
            var found = threadLocal.get();
            if (found != newContext) {
                threadLocal.set(originalContext.putResponseHeaders(found.responseHeaders));
            } else {
                threadLocal.set(originalContext);
            }
        };
    }

    public boolean hasTraceContext() {
        final ThreadContextStruct context = threadLocal.get();
        return context.requestHeaders.containsKey(Task.TRACE_PARENT_HTTP_HEADER)
            || context.requestHeaders.containsKey(Task.TRACE_STATE)
            || context.transientHeaders.containsKey(Task.APM_TRACE_CONTEXT);
    }

    /**
     * When using a {@link org.elasticsearch.tracing.Tracer}, sometimes you need to start a span completely unrelated
     * to any current span. In order to avoid any parent/child relationship being created, this method creates a new
     * context that clears all the tracing fields.
     *
     * @return a stored context, which can be restored when this context is no longer needed.
     */
    public StoredContext clearTraceContext() {
        final ThreadContextStruct context = threadLocal.get();
        final Map<String, String> newRequestHeaders = new HashMap<>(context.requestHeaders);
        final Map<String, Object> newTransientHeaders = new HashMap<>(context.transientHeaders);

        newRequestHeaders.remove(Task.TRACE_PARENT_HTTP_HEADER);
        newRequestHeaders.remove(Task.TRACE_STATE);

        newTransientHeaders.remove("parent_" + Task.TRACE_PARENT_HTTP_HEADER);
        newTransientHeaders.remove("parent_" + Task.TRACE_STATE);
        newTransientHeaders.remove(Task.APM_TRACE_CONTEXT);
        newTransientHeaders.remove("parent_" + Task.APM_TRACE_CONTEXT);

        threadLocal.set(
            new ThreadContextStruct(
                newRequestHeaders,
                context.responseHeaders,
                newTransientHeaders,
                context.isSystemContext,
                context.warningHeadersSize
            )
        );
        return storedOriginalContext(context);
    }

    private StoredContext storedOriginalContext(ThreadContextStruct originalContext) {
        return () -> threadLocal.set(originalContext);
    }

    private static Set<String> getRequestHeadersToCopy(Set<String> requestHeaders) {
        if (requestHeaders.isEmpty()) {
            return HEADERS_TO_COPY;
        }
        final Set<String> allRequestHeadersToCopy = new HashSet<>(requestHeaders);
        allRequestHeadersToCopy.addAll(HEADERS_TO_COPY);
        return Set.copyOf(allRequestHeadersToCopy);
    }

    private static Map<String, String> getHeadersPresentInContext(ThreadContextStruct context, Set<String> headers) {
        Map<String, String> map = Maps.newMapWithExpectedSize(headers.size());
        for (String header : headers) {
            final String value = context.requestHeaders.get(header);
            if (value != null) {
                map.put(header, value);
            }
        }
        return map;
    }

    /**
     * Captures the current thread context as writeable, allowing it to be serialized out later
     */
    public Writeable captureAsWriteable() {
        final ThreadContextStruct context = threadLocal.get();
        return out -> context.writeTo(out, defaultHeader);
    }

    /**
     * Removes the current context and resets a default context marked with as
     * originating from the supplied string. The removed context can be
     * restored by closing the returned {@link StoredContext}. Callers should
     * be careful to save the current context before calling this method and
     * restore it any listeners, likely with
     * {@link ContextPreservingActionListener}. Use {@link OriginSettingClient}
     * which can be used to do this automatically.
     * <p>
     * Without security the origin is ignored, but security uses it to authorize
     * actions that are made up of many sub-actions. These actions call
     * {@link #stashWithOrigin} before performing on behalf of a user that
     * should be allowed even if the user doesn't have permission to perform
     * those actions on their own.
     * <p>
     * For example, a user might not have permission to GET from the tasks index
     * but the tasks API will perform a get on their behalf using this method
     * if it can't find the task in memory.
     */
    public StoredContext stashWithOrigin(String origin) {
        final ThreadContext.StoredContext storedContext = stashContext();
        putTransient(ACTION_ORIGIN_TRANSIENT_NAME, origin);
        return storedContext;
    }

    /**
     * Removes the current context and resets a new context that contains a merge of the current headers and the given headers.
     * The removed context can be restored when closing the returned {@link StoredContext}. The merge strategy is that headers
     * that are already existing are preserved unless they are defaults.
     */
    public StoredContext stashAndMergeHeaders(Map<String, String> headers) {
        final ThreadContextStruct context = threadLocal.get();
        Map<String, String> newHeader = new HashMap<>(headers);
        newHeader.putAll(context.requestHeaders);
        threadLocal.set(DEFAULT_CONTEXT.putHeaders(newHeader));
        return storedOriginalContext(context);
    }

    /**
     * Just like {@link #stashContext()} but no default context is set and the response headers of the restore thread will be preserved.
     */
    public StoredContext newStoredContextPreservingResponseHeaders() {
        final ThreadContextStruct originalContext = threadLocal.get();
        return () -> {
            var found = threadLocal.get();
            if (found != originalContext) {
                threadLocal.set(originalContext.putResponseHeaders(found.responseHeaders));
            }
        };
    }

    /**
     * Just like {@link #stashContext()} but no default context is set.
     */
    public StoredContext newStoredContext() {
        final ThreadContextStruct originalContext = threadLocal.get();
        return storedOriginalContext(originalContext);
    }

    /**
     * Just like {@link #stashContext()} but no default context is set. Instead, the {@code transientHeadersToClear} argument can be used
     * to clear specific transient headers in the new context and {@code requestHeadersToClear} can be used to clear specific request
     * headers. All original headers (without the {@code responseHeaders}) are restored by closing the returned {@link StoredContext}.
     */
    public StoredContext newStoredContext(Collection<String> transientHeadersToClear, Collection<String> requestHeadersToClear) {
        return newStoredContext(false, transientHeadersToClear, requestHeadersToClear);
    }

    /**
     * Just like {@link #newStoredContext(Collection, Collection)} but all headers are restored to original,
     * except of {@code responseHeaders} which will be preserved from the restore thread.
     */
    public StoredContext newStoredContextPreservingResponseHeaders(
        Collection<String> transientHeadersToClear,
        Collection<String> requestHeadersToClear
    ) {
        return newStoredContext(true, transientHeadersToClear, requestHeadersToClear);
    }

    private StoredContext newStoredContext(
        boolean preserveResponseHeaders,
        Collection<String> transientHeadersToClear,
        Collection<String> requestHeadersToClear
    ) {
        final ThreadContextStruct originalContext = threadLocal.get();
        // clear specific transient headers from the current context
        Map<String, Object> newTransientHeaders = null;
        for (String transientHeaderToClear : transientHeadersToClear) {
            if (originalContext.transientHeaders.containsKey(transientHeaderToClear)) {
                if (newTransientHeaders == null) {
                    newTransientHeaders = new HashMap<>(originalContext.transientHeaders);
                }
                newTransientHeaders.remove(transientHeaderToClear);
            }
        }
        Map<String, String> newRequestHeaders = null;
        for (String requestHeaderToClear : requestHeadersToClear) {
            if (originalContext.requestHeaders.containsKey(requestHeaderToClear)) {
                if (newRequestHeaders == null) {
                    newRequestHeaders = new HashMap<>(originalContext.requestHeaders);
                }
                newRequestHeaders.remove(requestHeaderToClear);
            }
        }
        if (newTransientHeaders != null || newRequestHeaders != null) {
            ThreadContextStruct threadContextStruct = new ThreadContextStruct(
                newRequestHeaders != null ? newRequestHeaders : originalContext.requestHeaders,
                originalContext.responseHeaders,
                newTransientHeaders != null ? newTransientHeaders : originalContext.transientHeaders,
                originalContext.isSystemContext,
                originalContext.warningHeadersSize
            );
            threadLocal.set(threadContextStruct);
        }
        // this is the context when this method returns
        final ThreadContextStruct newContext = threadLocal.get();
        return () -> {
            if (preserveResponseHeaders && threadLocal.get() != newContext) {
                threadLocal.set(originalContext.putResponseHeaders(threadLocal.get().responseHeaders));
            } else {
                threadLocal.set(originalContext);
            }
        };
    }

    /**
     * Returns a supplier that gathers a {@link #newStoredContextPreservingResponseHeaders()} and restores it once the
     * returned supplier is invoked. The context returned from the supplier is a stored version of the
     * suppliers callers context that should be restored once the originally gathered context is not needed anymore.
     * For instance this method should be used like this:
     *
     * <pre>
     *     Supplier&lt;ThreadContext.StoredContext&gt; restorable = context.newRestorableContext(true);
     *     new Thread() {
     *         public void run() {
     *             try (ThreadContext.StoredContext ctx = restorable.get()) {
     *                 // execute with the parents context and restore the threads context afterwards
     *             }
     *         }
     *
     *     }.start();
     * </pre>
     *
     * @param preserveResponseHeaders if set to <code>true</code> the response headers of the restore thread will be preserved.
     * @return a restorable context supplier
     */
    public Supplier<StoredContext> newRestorableContext(boolean preserveResponseHeaders) {
        return wrapRestorable(preserveResponseHeaders ? newStoredContextPreservingResponseHeaders() : newStoredContext());
    }

    /**
     * Same as {@link #newRestorableContext(boolean)} but wraps an existing context to restore.
     * @param storedContext the context to restore
     */
    public Supplier<StoredContext> wrapRestorable(StoredContext storedContext) {
        return () -> {
            StoredContext context = newStoredContext();
            storedContext.restore();
            return context;
        };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        threadLocal.get().writeTo(out, defaultHeader);
    }

    /**
     * Reads the headers from the stream into the current context
     */
    public void readHeaders(StreamInput in) throws IOException {
        setHeaders(readHeadersFromStream(in));
    }

    public void setHeaders(Tuple<Map<String, String>, Map<String, Set<String>>> headerTuple) {
        final Map<String, String> requestHeaders = headerTuple.v1();
        final Map<String, Set<String>> responseHeaders = headerTuple.v2();
        final ThreadContextStruct struct;
        if (requestHeaders.isEmpty() && responseHeaders.isEmpty()) {
            struct = ThreadContextStruct.EMPTY;
        } else {
            struct = new ThreadContextStruct(requestHeaders, responseHeaders, Collections.emptyMap(), false);
        }
        threadLocal.set(struct);
    }

    public static Tuple<Map<String, String>, Map<String, Set<String>>> readHeadersFromStream(StreamInput in) throws IOException {
        final Map<String, String> requestHeaders = in.readMap(StreamInput::readString, StreamInput::readString);
        final Map<String, Set<String>> responseHeaders = in.readMap(StreamInput::readString, input -> {
            final int size = input.readVInt();
            if (size == 0) {
                return Collections.emptySet();
            } else if (size == 1) {
                return Collections.singleton(input.readString());
            } else {
                // use a linked hash set to preserve order
                final LinkedHashSet<String> values = Sets.newLinkedHashSetWithExpectedSize(size);
                for (int i = 0; i < size; i++) {
                    final String value = input.readString();
                    final boolean added = values.add(value);
                    assert added : value;
                }
                return values;
            }
        });
        return new Tuple<>(requestHeaders, responseHeaders);
    }

    /**
     * Returns the header for the given key or <code>null</code> if not present
     */
    public String getHeader(String key) {
        String value = threadLocal.get().requestHeaders.get(key);
        if (value == null) {
            return defaultHeader.get(key);
        }
        return value;
    }

    /**
     * Returns all of the request headers from the thread's context.<br>
     * <b>Be advised, headers might contain credentials.</b>
     * In order to avoid storing, and erroneously exposing, such headers,
     * it is recommended to instead store security headers that prove
     * the credentials have been verified successfully, and which are
     * internal to the system, in the sense that they cannot be sent
     * by the clients.
     */
    public Map<String, String> getHeaders() {
        HashMap<String, String> map = new HashMap<>(defaultHeader);
        map.putAll(threadLocal.get().requestHeaders);
        return Collections.unmodifiableMap(map);
    }

    /**
     * Returns the request headers, without the default headers
     */
    public Map<String, String> getRequestHeadersOnly() {
        return Collections.unmodifiableMap(new HashMap<>(threadLocal.get().requestHeaders));
    }

    /**
     * Get a copy of all <em>response</em> headers.
     *
     * @return Never {@code null}.
     */
    public Map<String, List<String>> getResponseHeaders() {
        Map<String, Set<String>> responseHeaders = threadLocal.get().responseHeaders;
        Map<String, List<String>> map = Maps.newMapWithExpectedSize(responseHeaders.size());

        for (Map.Entry<String, Set<String>> entry : responseHeaders.entrySet()) {
            map.put(entry.getKey(), List.copyOf(entry.getValue()));
        }

        return Collections.unmodifiableMap(map);
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
        threadLocal.set(threadLocal.get().putRequest(key, value));
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
    @SuppressWarnings("unchecked") // (T)object
    public <T> T getTransient(String key) {
        return (T) threadLocal.get().transientHeaders.get(key);
    }

    /**
     * Returns unmodifiable copy of all transient headers.
     */
    public Map<String, Object> getTransientHeaders() {
        return Collections.unmodifiableMap(threadLocal.get().transientHeaders);
    }

    /**
     * Add the {@code value} for the specified {@code key} Any duplicate {@code value} is ignored.
     *
     * @param key         the header name
     * @param value       the header value
     */
    public void addResponseHeader(final String key, final String value) {
        addResponseHeader(key, value, v -> v);
    }

    /**
     * Add the {@code value} for the specified {@code key} with the specified {@code uniqueValue} used for de-duplication. Any duplicate
     * {@code value} after applying {@code uniqueValue} is ignored.
     *
     * @param key         the header name
     * @param value       the header value
     * @param uniqueValue the function that produces de-duplication values
     */
    public void addResponseHeader(final String key, final String value, final Function<String, String> uniqueValue) {
        threadLocal.set(threadLocal.get().putResponse(key, value, uniqueValue, maxWarningHeaderCount, maxWarningHeaderSize));
    }

    /**
     * Saves the current thread context and wraps command in a Runnable that restores that context before running command. If
     * <code>command</code> has already been passed through this method then it is returned unaltered rather than wrapped twice.
     */
    public Runnable preserveContext(Runnable command) {
        return doPreserveContext(command, false);
    }

    /**
     * Saves the current thread context and wraps command in a Runnable that restores that context before running command. Also
     * starts a new tracing context durin executing. If <code>command</code> has already been wrapped then it is returned unaltered.
     */
    public Runnable preserveContextWithTracing(Runnable command) {
        return doPreserveContext(command, true);
    }

    private Runnable doPreserveContext(Runnable command, boolean preserveContext) {
        if (command instanceof ContextPreservingAbstractRunnable) {
            return command;
        }
        if (command instanceof ContextPreservingRunnable) {
            return command;
        }
        if (command instanceof AbstractRunnable abstractRunnable) {
            return new ContextPreservingAbstractRunnable(abstractRunnable, preserveContext);
        }
        return new ContextPreservingRunnable(command);
    }

    /**
     * Unwraps a command that was previously wrapped by {@link #preserveContext(Runnable)}.
     */
    public static Runnable unwrap(Runnable command) {
        if (command instanceof WrappedRunnable) {
            return ((WrappedRunnable) command).unwrap();
        }
        return command;
    }

    /**
     * Returns true if the current context is the default context.
     */
    public boolean isDefaultContext() {
        return threadLocal.get() == DEFAULT_CONTEXT;
    }

    /**
     * Marks this thread context as an internal system context. This signals that actions in this context are issued
     * by the system itself rather than by a user action.
     */
    public void markAsSystemContext() {
        threadLocal.set(threadLocal.get().setSystemContext());
    }

    /**
     * Returns <code>true</code> iff this context is a system context
     */
    public boolean isSystemContext() {
        return threadLocal.get().isSystemContext;
    }

    /**
     * Remove unwanted and unneeded headers from the thread context. Does not store prior context.
     */
    public void sanitizeHeaders() {
        final ThreadContextStruct originalContext = threadLocal.get();
        final Map<String, String> newRequestHeaders = new HashMap<>(originalContext.requestHeaders);

        newRequestHeaders.entrySet()
            .removeIf(
                entry -> entry.getKey().equalsIgnoreCase("authorization")
                    || entry.getKey().equalsIgnoreCase("es-secondary-authorization")
                    || entry.getKey().equalsIgnoreCase("ES-Client-Authentication")
            );

        final ThreadContextStruct newContext = new ThreadContextStruct(
            newRequestHeaders,
            originalContext.responseHeaders,
            originalContext.transientHeaders,
            originalContext.isSystemContext,
            originalContext.warningHeadersSize
        );
        threadLocal.set(newContext);
        // intentionally not storing prior context to avoid restoring unwanted headers
    }

    @FunctionalInterface
    public interface StoredContext extends AutoCloseable, Releasable {
        default void restore() {
            close();
        }
    }

    public static Map<String, String> buildDefaultHeaders(Settings settings) {
        Settings headers = DEFAULT_HEADERS_SETTING.get(settings);
        if (headers == null) {
            return Map.of();
        } else {
            Map<String, String> defaultHeader = new HashMap<>();
            for (String key : headers.names()) {
                defaultHeader.put(key, headers.get(key));
            }
            return Map.copyOf(defaultHeader);
        }
    }

    private static final class ThreadContextStruct {

        private static final ThreadContextStruct EMPTY = new ThreadContextStruct(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            false
        );

        private final Map<String, String> requestHeaders;
        private final Map<String, Object> transientHeaders;
        private final Map<String, Set<String>> responseHeaders;
        private final boolean isSystemContext;
        // saving current warning headers' size not to recalculate the size with every new warning header
        private final long warningHeadersSize;

        private ThreadContextStruct setSystemContext() {
            if (isSystemContext) {
                return this;
            }
            return new ThreadContextStruct(requestHeaders, responseHeaders, transientHeaders, true);
        }

        private ThreadContextStruct(
            Map<String, String> requestHeaders,
            Map<String, Set<String>> responseHeaders,
            Map<String, Object> transientHeaders,
            boolean isSystemContext
        ) {
            this(requestHeaders, responseHeaders, transientHeaders, isSystemContext, 0L);
        }

        private ThreadContextStruct(
            Map<String, String> requestHeaders,
            Map<String, Set<String>> responseHeaders,
            Map<String, Object> transientHeaders,
            boolean isSystemContext,
            long warningHeadersSize
        ) {
            this.requestHeaders = requestHeaders;
            this.responseHeaders = responseHeaders;
            this.transientHeaders = transientHeaders;
            this.isSystemContext = isSystemContext;
            this.warningHeadersSize = warningHeadersSize;
        }

        /**
         * This represents the default context and it should only ever be called by {@link #DEFAULT_CONTEXT}.
         */
        private ThreadContextStruct() {
            this(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), false);
        }

        private ThreadContextStruct putRequest(String key, String value) {
            Map<String, String> newRequestHeaders = new HashMap<>(this.requestHeaders);
            putSingleHeader(key, value, newRequestHeaders);
            return new ThreadContextStruct(newRequestHeaders, responseHeaders, transientHeaders, isSystemContext);
        }

        private static <T> void putSingleHeader(String key, T value, Map<String, T> newHeaders) {
            if (newHeaders.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException("value for key [" + key + "] already present");
            }
        }

        private ThreadContextStruct putHeaders(Map<String, String> headers) {
            if (headers.isEmpty()) {
                return this;
            } else {
                final Map<String, String> newHeaders = new HashMap<>(this.requestHeaders);
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    putSingleHeader(entry.getKey(), entry.getValue(), newHeaders);
                }
                return new ThreadContextStruct(newHeaders, responseHeaders, transientHeaders, isSystemContext);
            }
        }

        private ThreadContextStruct putResponseHeaders(Map<String, Set<String>> headers) {
            assert headers != null;
            if (headers.isEmpty()) {
                return this;
            }
            final Map<String, Set<String>> newResponseHeaders = new HashMap<>(this.responseHeaders);
            for (Map.Entry<String, Set<String>> entry : headers.entrySet()) {
                newResponseHeaders.merge(entry.getKey(), entry.getValue(), (existing, added) -> {
                    final Set<String> updated = new LinkedHashSet<>(added);
                    updated.addAll(existing);
                    return Collections.unmodifiableSet(updated);
                });
            }
            return new ThreadContextStruct(requestHeaders, newResponseHeaders, transientHeaders, isSystemContext);
        }

        private ThreadContextStruct putResponse(
            final String key,
            final String value,
            final Function<String, String> uniqueValue,
            final int maxWarningHeaderCount,
            final long maxWarningHeaderSize
        ) {
            assert value != null;
            long newWarningHeaderSize = warningHeadersSize;
            // check if we can add another warning header - if max size within limits
            if (key.equals("Warning") && (maxWarningHeaderSize != -1)) { // if size is NOT unbounded, check its limits
                if (warningHeadersSize > maxWarningHeaderSize) { // if max size has already been reached before
                    logger.warn(
                        "Dropping a warning header, as their total size reached the maximum allowed of ["
                            + maxWarningHeaderSize
                            + "] bytes set in ["
                            + HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_SIZE.getKey()
                            + "]!"
                    );
                    return this;
                }
                newWarningHeaderSize += "Warning".getBytes(StandardCharsets.UTF_8).length + value.getBytes(StandardCharsets.UTF_8).length;
                if (newWarningHeaderSize > maxWarningHeaderSize) {
                    logger.warn(
                        "Dropping a warning header, as their total size reached the maximum allowed of ["
                            + maxWarningHeaderSize
                            + "] bytes set in ["
                            + HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_SIZE.getKey()
                            + "]!"
                    );
                    return new ThreadContextStruct(
                        requestHeaders,
                        responseHeaders,
                        transientHeaders,
                        isSystemContext,
                        newWarningHeaderSize
                    );
                }
            }

            final Map<String, Set<String>> newResponseHeaders;
            final Set<String> existingValues = responseHeaders.get(key);
            if (existingValues != null && existingValues.contains(uniqueValue.apply(value))) {
                return this;
            }
            newResponseHeaders = new HashMap<>(responseHeaders);
            if (existingValues != null) {
                // preserve insertion order
                final Set<String> newValues = new LinkedHashSet<>(existingValues);
                newValues.add(value);
                newResponseHeaders.put(key, Collections.unmodifiableSet(newValues));
            } else {
                newResponseHeaders.put(key, Collections.singleton(value));
            }

            // check if we can add another warning header - if max count within limits
            if ((key.equals("Warning")) && (maxWarningHeaderCount != -1)) { // if count is NOT unbounded, check its limits
                final int warningHeaderCount = newResponseHeaders.containsKey("Warning") ? newResponseHeaders.get("Warning").size() : 0;
                if (warningHeaderCount > maxWarningHeaderCount) {
                    logger.warn(
                        "Dropping a warning header, as their total count reached the maximum allowed of ["
                            + maxWarningHeaderCount
                            + "] set in ["
                            + HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_COUNT.getKey()
                            + "]!"
                    );
                    return this;
                }
            }
            return new ThreadContextStruct(requestHeaders, newResponseHeaders, transientHeaders, isSystemContext, newWarningHeaderSize);
        }

        private ThreadContextStruct putTransient(String key, Object value) {
            Map<String, Object> newTransient = new HashMap<>(this.transientHeaders);
            putSingleHeader(key, value, newTransient);
            return new ThreadContextStruct(requestHeaders, responseHeaders, newTransient, isSystemContext);
        }

        private ThreadContextStruct copyHeaders(Iterable<Map.Entry<String, String>> headers) {
            Map<String, String> newHeaders = new HashMap<>();
            for (Map.Entry<String, String> header : headers) {
                newHeaders.put(header.getKey(), header.getValue());
            }
            return putHeaders(newHeaders);
        }

        private void writeTo(StreamOutput out, Map<String, String> defaultHeaders) throws IOException {
            final Map<String, String> requestHeaders;
            if (defaultHeaders.isEmpty()) {
                requestHeaders = this.requestHeaders;
            } else {
                requestHeaders = new HashMap<>(defaultHeaders);
                requestHeaders.putAll(this.requestHeaders);
            }

            out.writeMap(requestHeaders, StreamOutput::writeString, StreamOutput::writeString);
            out.writeMap(responseHeaders, StreamOutput::writeString, StreamOutput::writeStringCollection);
        }
    }

    /**
     * Wraps a Runnable to preserve the thread context.
     */
    private class ContextPreservingRunnable implements WrappedRunnable {
        private final Runnable in;
        private final ThreadContext.StoredContext ctx;

        private ContextPreservingRunnable(Runnable in) {
            ctx = newStoredContext();
            this.in = in;
        }

        @Override
        public void run() {
            try (ThreadContext.StoredContext ignore = stashContext()) {
                ctx.restore();
                in.run();
            }
        }

        @Override
        public String toString() {
            return in.toString();
        }

        @Override
        public Runnable unwrap() {
            return in;
        }
    }

    /**
     * Wraps an AbstractRunnable to preserve the thread context, optionally creating a new trace context before
     * executing.
     */
    private class ContextPreservingAbstractRunnable extends AbstractRunnable implements WrappedRunnable {
        private final AbstractRunnable in;
        private final ThreadContext.StoredContext creatorsContext;
        private final boolean useNewTraceContext;

        private ThreadContext.StoredContext threadsOriginalContext = null;

        private ContextPreservingAbstractRunnable(AbstractRunnable in, boolean useNewTraceContext) {
            creatorsContext = newStoredContext();
            this.in = in;
            this.useNewTraceContext = useNewTraceContext;
        }

        @Override
        public boolean isForceExecution() {
            return in.isForceExecution();
        }

        @Override
        public void onAfter() {
            try {
                in.onAfter();
            } finally {
                if (threadsOriginalContext != null) {
                    threadsOriginalContext.restore();
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            in.onFailure(e);
        }

        @Override
        public void onRejection(Exception e) {
            in.onRejection(e);
        }

        @Override
        protected void doRun() throws Exception {
            threadsOriginalContext = stashContext();
            creatorsContext.restore();
            if (useNewTraceContext) {
                // Discard the return value - we'll restore threadsOriginalContext in `onAfter()`.
                // noinspection resource
                newTraceContext();
            }
            in.doRun();
        }

        @Override
        public String toString() {
            return in.toString();
        }

        @Override
        public AbstractRunnable unwrap() {
            return in;
        }
    }

}
