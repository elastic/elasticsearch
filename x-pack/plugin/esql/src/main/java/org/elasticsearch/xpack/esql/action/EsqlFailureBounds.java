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
 * before it is returned or stored, and logs that it did. It does the same for a graph that does not loop but would
 * still render too much (see {@link #MAX_RENDERED_WEIGHT}). Graphs that merely share an exception are otherwise left
 * alone.
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
     * Rendering weight a failure may have before it is rebuilt: the sum, over every entry a renderer writes, of the
     * entry's depth plus one. With {@code error_trace} each entry's stack trace also prints the entry's own subgraph, so
     * the response grows with this sum rather than with the number of entries (a few KB per unit with typical stack
     * depths, so roughly 20 MB at the limit). A failure collector holding ten collectors of ten failures each weighs 321
     * and passes; thirty of thirty weighs 2761 and is rebuilt, as is any loop.
     */
    static final int MAX_RENDERED_WEIGHT = 2500;

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
     * Returns {@code failure} unchanged unless its exception graph loops or weighs more than
     * {@link #MAX_RENDERED_WEIGHT} to render. Otherwise returns a rebuilt failure that references none of the original
     * exceptions: it has the same message, stack trace and status as {@code failure} (a {@link CircuitBreakingException}
     * stays one), keeps up to {@link #MAX_CAUSE_CHAIN} links of its cause chain, rebuilt the same way, and carries up to
     * {@link #MAX_ADDITIONAL_FAILURES} further distinct failures from the graph, rebuilt the same way, as suppressed.
     */
    public static Exception bound(Exception failure, String query) {
        Map<Throwable, RenderCost> memo = new IdentityHashMap<>();
        Set<Throwable> onPath = Collections.newSetFromMap(new IdentityHashMap<>());
        if (renderCost(failure, memo, onPath).weight() <= MAX_RENDERED_WEIGHT) {
            return failure;
        }
        List<Throwable> distinct = distinct(failure);
        logger.warn(
            "query [{}] failed with an exception graph that loops or weighs more than [{}] to render; rebuilding it from [{}] distinct "
                + "exceptions",
            query,
            MAX_RENDERED_WEIGHT,
            distinct.size()
        );
        // causes beyond the kept links are dropped with their chain, so they never take a slot of their own
        Set<Throwable> inChain = Collections.newSetFromMap(new IdentityHashMap<>());
        inChain.addAll(causeChain(failure, Integer.MAX_VALUE));
        // the renderer reports the first non-wrapper exception as the error, so the rebuilt failure starts there too
        Exception bounded = rebuild(causeChain(ExceptionsHelper.unwrapCause(failure), MAX_CAUSE_CHAIN));
        int added = 0;
        for (Throwable t : distinct) {
            if (added == MAX_ADDITIONAL_FAILURES) {
                break;
            }
            if (inChain.contains(t) == false) {
                inChain.addAll(causeChain(t, Integer.MAX_VALUE));
                bounded.addSuppressed(rebuild(causeChain(t, MAX_CAUSE_CHAIN)));
                added++;
            }
        }
        return bounded;
    }

    /**
     * What a renderer that follows every cause and suppressed link, without remembering what it wrote, writes for one
     * exception: {@code entries} is how many entries, {@code weight} the sum over them of their depth below that
     * exception plus one. Both are capped just above {@link #MAX_RENDERED_WEIGHT}.
     */
    private record RenderCost(long entries, long weight) {
        private static final long OVER = MAX_RENDERED_WEIGHT + 1L;
        private static final RenderCost EXCESSIVE = new RenderCost(OVER, OVER);
    }

    /**
     * The {@link RenderCost} of {@code t}; a loop is always {@link RenderCost#EXCESSIVE}. The renderer's own depth limit
     * is deliberately ignored: a cost memoised below that limit would be reused above it, where the renderer expands
     * further. Capping is safe because the weight of an exception is at least its entries and at least the weight of
     * any child, so a capped child always makes its parent excessive too.
     */
    private static RenderCost renderCost(Throwable t, Map<Throwable, RenderCost> memo, Set<Throwable> onPath) {
        // a path this long weighs more than the limit anyway; stopping here also bounds the recursion
        if (onPath.contains(t) || onPath.size() >= MAX_RENDERED_WEIGHT) {
            return RenderCost.EXCESSIVE;
        }
        RenderCost known = memo.get(t);
        if (known != null) {
            return known;
        }
        onPath.add(t);
        long entries = 1;
        long weight = 1;
        Throwable cause = t.getCause();
        if (cause != null) {
            RenderCost child = renderCost(cause, memo, onPath);
            entries += child.entries();
            weight += child.weight() + child.entries();
        }
        for (Throwable suppressed : t.getSuppressed()) {
            if (weight > MAX_RENDERED_WEIGHT) {
                break;
            }
            RenderCost child = renderCost(suppressed, memo, onPath);
            entries += child.entries();
            weight += child.weight() + child.entries();
        }
        onPath.remove(t);
        RenderCost cost = weight > MAX_RENDERED_WEIGHT ? RenderCost.EXCESSIVE : new RenderCost(entries, weight);
        memo.put(t, cost);
        return cost;
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

    /** {@code t} followed by its causes, stopping at a repeat or after {@code maxLinks} links. */
    private static List<Throwable> causeChain(Throwable t, int maxLinks) {
        List<Throwable> chain = new ArrayList<>();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable current = t; current != null && chain.size() <= maxLinks && seen.add(current); current = current.getCause()) {
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
            super(describe(original), cause);
            this.status = ExceptionsHelper.status(original);
        }

        private static String describe(Throwable original) {
            String type = "[" + typeName(original) + "]";
            return original.getMessage() == null ? type : type + " " + original.getMessage();
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
