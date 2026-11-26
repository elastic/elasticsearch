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
 * Opens and closes the snapshot of a query, see {@link QuerySnapshot}.
 */
final class QuerySnapshots {
    private static final Logger logger = LogManager.getLogger(QuerySnapshots.class);

    private QuerySnapshots() {}

    /**
     * The point in time of a query's snapshot. can_match skips shards with {@code filter}, slices are routed as in
     * {@link DataNodeRequestSender#searchShardsRequest}, and it fails if a shard fails.
     *
     * @param keepAlive how long a reader lives without being used
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
     * Opens the point in time as a child of the query's task. It's closed if the task is cancelled, otherwise by
     * {@link #close}.
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
                QuerySnapshot snapshot = new QuerySnapshot(
                    response.getPointInTimeId(),
                    id.shards(),
                    response.getTotalShards(),
                    response.getSkippedShards(),
                    owner
                );
                task.addListener(() -> close(client, snapshot));
                if (task.notifyIfCancelled(l) == false) {
                    l.onResponse(snapshot);
                }
            })
        );
    }

    /**
     * Closes the point in time as the user who runs the query, without waiting. Only the first close acts.
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
