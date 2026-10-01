/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.compute.operator.SuppressedFailures;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Last line of defence against a failed query's error being an exception graph too large to render.
 * <p>
 * The REST renderer and the transport serializer bound the depth of an exception graph but keep no record of what
 * they have already written, so an exception reachable along several paths through cause and suppressed links is
 * written once per path. In a loop that also branches (A suppressing B twice and B suppressing A) the number of paths
 * grows exponentially with the depth the renderer allows, and rendering it can exhaust the heap of the node that
 * builds the response. ES|QL is
 * not supposed to build loops (see {@link SuppressedFailures}); this replaces such a graph with a clean equivalent
 * before it is returned or stored, and logs that it did. Graphs that merely share an exception, without looping and
 * without rendering an excessive number of entries, are left alone.
 * <p>
 * Not covered: {@code TransportEsqlStreamQueryAction}, and failures reported inside a successful response as partial
 * results ({@code ShardSearchFailure}).
 */
public final class EsqlFailureBounds {

    private static final Logger logger = LogManager.getLogger(EsqlFailureBounds.class);

    /**
     * Further distinct failures carried by a rebuilt failure. Together with the rebuilt failure itself this matches the
     * default budget of {@code FailureCollector}, so the bounded error is no smaller than what one collector produces.
     */
    static final int MAX_ADDITIONAL_FAILURES = 9;

    /**
     * Entries a renderer may write for one failure before it is rebuilt. Far above what nested failure collectors
     * legitimately produce (tens to a few hundred), far below what a loop produces. This bounds entries, not bytes: with
     * {@code error_trace} each entry's stack trace also prints the entry's own subgraph.
     */
    static final int MAX_RENDERED_ENTRIES = 1000;

    /** Cause links kept per rebuilt failure. */
    static final int MAX_CAUSE_CHAIN = 10;

    private EsqlFailureBounds() {}

    /**
     * Wraps {@code listener} so that any failure it receives goes through {@link #bound} first.
     */
    public static <T> ActionListener<T> wrap(ActionListener<T> listener, String query) {
        return listener.delegateResponse((l, e) -> l.onFailure(bound(e, query)));
    }

    /**
     * Returns {@code failure} unchanged unless its exception graph loops or would render more than
     * {@link #MAX_RENDERED_ENTRIES} entries. Otherwise returns a rebuilt failure that references none of the original
     * exceptions: it has the same message, stack trace and status as {@code failure} (a {@link CircuitBreakingException}
     * stays one), keeps up to {@link #MAX_CAUSE_CHAIN} links of its cause chain, rebuilt the same way, and carries up to
     * {@link #MAX_ADDITIONAL_FAILURES} further distinct failures from the graph, rebuilt the same way, as suppressed.
     */
    public static Exception bound(Exception failure, String query) {
        Map<Throwable, Long> entries = new IdentityHashMap<>();
        Set<Throwable> onPath = Collections.newSetFromMap(new IdentityHashMap<>());
        if (renderedEntries(failure, entries, onPath) <= MAX_RENDERED_ENTRIES) {
            return failure;
        }
        List<Throwable> distinct = distinct(failure);
        logger.warn(
            "query [{}] failed with an exception graph that loops or renders more than [{}] entries; rebuilding it from [{}] distinct "
                + "exceptions",
            query,
            MAX_RENDERED_ENTRIES,
            distinct.size()
        );
        Set<Throwable> inChain = Collections.newSetFromMap(new IdentityHashMap<>());
        inChain.addAll(causeChain(failure));
        // the renderer reports the first non-wrapper exception as the error, so the rebuilt failure starts there too
        List<Throwable> chain = causeChain(ExceptionsHelper.unwrapCause(failure));
        inChain.addAll(chain);
        Exception bounded = rebuild(chain);
        int added = 0;
        for (Throwable t : distinct) {
            if (added == MAX_ADDITIONAL_FAILURES) {
                break;
            }
            if (inChain.contains(t) == false) {
                List<Throwable> suppressedChain = causeChain(t);
                inChain.addAll(suppressedChain);
                bounded.addSuppressed(rebuild(suppressedChain));
                added++;
            }
        }
        return bounded;
    }

    /**
     * How many entries a renderer that follows every cause and suppressed link, without remembering what it wrote,
     * writes for {@code t}; capped just above {@link #MAX_RENDERED_ENTRIES}, which a loop always reaches. The renderer's
     * own depth limit is deliberately ignored: a count memoised below that limit would be reused above it, where the
     * renderer expands further.
     */
    private static long renderedEntries(Throwable t, Map<Throwable, Long> memo, Set<Throwable> onPath) {
        // a path this long renders more entries than the limit anyway; stopping here also bounds the recursion
        if (onPath.contains(t) || onPath.size() >= MAX_RENDERED_ENTRIES) {
            return MAX_RENDERED_ENTRIES + 1;
        }
        Long known = memo.get(t);
        if (known != null) {
            return known;
        }
        onPath.add(t);
        long count = 1;
        Throwable cause = t.getCause();
        if (cause != null) {
            count += renderedEntries(cause, memo, onPath);
        }
        for (Throwable suppressed : t.getSuppressed()) {
            if (count > MAX_RENDERED_ENTRIES) {
                break;
            }
            count += renderedEntries(suppressed, memo, onPath);
        }
        onPath.remove(t);
        count = Math.min(count, MAX_RENDERED_ENTRIES + 1);
        memo.put(t, count);
        return count;
    }

    /** Every exception reachable from {@code root}, once each, starting with {@code root}. */
    private static List<Throwable> distinct(Throwable root) {
        List<Throwable> distinct = new ArrayList<>();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Throwable> pending = new ArrayDeque<>();
        pending.push(root);
        while (pending.isEmpty() == false) {
            Throwable current = pending.pop();
            if (seen.add(current) == false) {
                continue;
            }
            distinct.add(current);
            Throwable[] suppressed = current.getSuppressed();
            for (int i = suppressed.length - 1; i >= 0; i--) {
                pending.push(suppressed[i]);
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.push(cause);
            }
        }
        return distinct;
    }

    /** {@code t} followed by its causes, stopping at a repeat or after {@link #MAX_CAUSE_CHAIN} links. */
    private static List<Throwable> causeChain(Throwable t) {
        List<Throwable> chain = new ArrayList<>();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable current = t; current != null && chain.size() <= MAX_CAUSE_CHAIN && seen.add(current); current = current.getCause()) {
            chain.add(current);
        }
        return chain;
    }

    private static Exception rebuild(List<Throwable> chain) {
        Exception rebuilt = null;
        for (int i = chain.size() - 1; i >= 0; i--) {
            rebuilt = copy(chain.get(i), rebuilt);
        }
        return rebuilt;
    }

    private static Exception copy(Throwable original, @Nullable Exception cause) {
        Exception copy;
        if (original instanceof CircuitBreakingException cbe) {
            copy = new CircuitBreakingException(cbe.getMessage(), cbe.getBytesWanted(), cbe.getByteLimit(), cbe.getDurability());
            if (cause != null) {
                copy.initCause(cause);
            }
        } else {
            copy = new BoundedFailureException(original, cause);
        }
        copy.setStackTrace(original.getStackTrace());
        return copy;
    }

    /**
     * Stands in for an original failure of any type other than {@link CircuitBreakingException}. Named rather than
     * anonymous because the rendered {@code type} comes from the class's simple name.
     */
    static final class BoundedFailureException extends ElasticsearchException {
        private final RestStatus status;

        BoundedFailureException(Throwable original, @Nullable Exception cause) {
            super(original.getMessage() == null ? "[{}]" : "[{}] {}", cause, typeName(original), original.getMessage());
            this.status = ExceptionsHelper.status(original);
        }

        private static String typeName(Throwable original) {
            return original instanceof NotSerializableExceptionWrapper wrapper
                ? wrapper.getExceptionName()
                : ElasticsearchException.getExceptionName(original);
        }

        @Override
        public RestStatus status() {
            return status;
        }
    }
}
