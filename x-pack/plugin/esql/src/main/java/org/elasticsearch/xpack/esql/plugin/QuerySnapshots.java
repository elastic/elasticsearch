/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.SliceSelection;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;

import java.util.function.Supplier;

/**
 * Opens and closes the snapshot of a query, see {@link QuerySnapshot}: a point in time on the indices the query reads.
 */
final class QuerySnapshots {
    private static final Logger logger = LogManager.getLogger(QuerySnapshots.class);

    private QuerySnapshots() {}

    /**
     * The point in time of a query's snapshot: on its indices, with the filter can_match skips shards with, and the slices
     * it reads as {@link DataNodeRequestSender#searchShardsRequest} routes them. It fails if a shard fails to open.
     *
     * @param indices         the indices the query reads
     * @param filter          the filter can_match skips shards with, or {@code null} for none
     * @param anySliceEnabled whether any of the indices is slice-enabled, without which slices aren't routed
     * @param keepAlive       how long a reader lives without being used: it bounds how long the readers outlive a query
     *                        whose node fails before it closes them, and a read after a longer pause fails
     */
    static OpenPointInTimeRequest openRequest(
        String[] indices,
        IndicesOptions indicesOptions,
        @Nullable QueryBuilder filter,
        SliceSelection slices,
        boolean anySliceEnabled,
        TimeValue keepAlive
    ) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).indicesOptions(indicesOptions)
            .keepAlive(keepAlive)
            .allowPartialSearchResults(false);
        request.indexFilter(filter);
        if (anySliceEnabled && slices.isRestricted()) {
            request.routing(slices.toRouting()).setRoutingFromSlice(true);
        }
        return request;
    }

    /**
     * Opens the point in time of {@code request} as a child of the query's task, so it's authorized as the user who runs
     * the query. The snapshot is closed when the task is cancelled, even if it was cancelled while the point in time
     * opened; otherwise the caller closes it with {@link #close}.
     *
     * @param localNodeId the node the query's task runs on
     */
    static void open(
        Client client,
        NamedWriteableRegistry registry,
        String localNodeId,
        CancellableTask task,
        OpenPointInTimeRequest request,
        ActionListener<QuerySnapshot> listener
    ) {
        Supplier<ThreadContext.StoredContext> owner = client.threadPool().getThreadContext().newRestorableContext(false);
        TaskId parent = new TaskId(localNodeId, task.getId());
        new ParentTaskAssigningClient(client, parent).execute(
            TransportOpenPointInTimeAction.TYPE,
            request,
            listener.delegateFailureAndWrap((l, response) -> {
                SearchContextId id = SearchContextId.decode(registry, response.getPointInTimeId());
                QuerySnapshot snapshot = new QuerySnapshot(response.getPointInTimeId(), id.shards(), owner);
                task.addListener(() -> close(client, snapshot));
                if (task.notifyIfCancelled(l) == false) {
                    l.onResponse(snapshot);
                }
            })
        );
    }

    /**
     * Closes the point in time of the snapshot as the user who runs the query, without waiting for it. Only the first
     * close acts; readers the close misses expire with the keep-alive.
     */
    static void close(Client client, QuerySnapshot snapshot) {
        if (snapshot.markClosed() == false) {
            return;
        }
        try (ThreadContext.StoredContext ignored = snapshot.owner().get()) {
            client.execute(
                TransportClosePointInTimeAction.TYPE,
                new ClosePointInTimeRequest(snapshot.pointInTimeId()),
                ActionListener.wrap(r -> {}, e -> logger.debug("failed to close the point in time of a query's snapshot", e))
            );
        }
    }
}
