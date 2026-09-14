/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

/** Reads the index shape and settings behind {@link KnnEvalEnvironment}; every lookup is a nice-to-have that never fails the request. */
final class KnnEvalEnvironmentResolver {

    private static final Logger logger = LogManager.getLogger(KnnEvalEnvironmentResolver.class);

    /** For the metadata lookups behind the reported environment, whose result is a nice-to-have. */
    private static final TimeValue MASTER_TIMEOUT = TimeValue.timeValueSeconds(30);

    private final Client client;
    private final ClusterService clusterService;

    KnnEvalEnvironmentResolver(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    /**
     * Describes what the sweep is about to measure on. Segment layout and index version both need {@code monitor} privileges, so each
     * is dropped on failure rather than costing the caller their recall numbers.
     */
    void resolve(KnnEvalRequest request, KnnEvalFieldContext fieldContext, ActionListener<KnnEvalFieldContext> listener) {
        boolean allowExpensiveQueries = clusterService.getClusterSettings().get(SearchService.ALLOW_EXPENSIVE_QUERIES);
        client.execute(
            IndicesSegmentsAction.INSTANCE,
            new IndicesSegmentsRequest(request.indices()).indicesOptions(request.indicesOptions()),
            withoutFailing(
                logger,
                "index segments",
                segments -> resolveIndexVersions(
                    request,
                    versions -> listener.onResponse(
                        fieldContext.withEnvironment(
                            new KnnEvalEnvironment(indexSummary(segments), fieldContext.field(), versions, allowExpensiveQueries)
                        )
                    )
                )
            )
        );
    }

    private void resolveIndexVersions(KnnEvalRequest request, Consumer<List<String>> onVersions) {
        GetSettingsRequest settingsRequest = new GetSettingsRequest(MASTER_TIMEOUT).indices(request.indices())
            .indicesOptions(request.indicesOptions())
            .names(IndexMetadata.SETTING_VERSION_CREATED);
        client.execute(GetSettingsAction.INSTANCE, settingsRequest, withoutFailing(logger, "index settings", settings -> {
            if (settings == null) {
                onVersions.accept(List.of());
                return;
            }
            Set<String> versions = new TreeSet<>();
            for (Settings indexSettings : settings.getIndexToSettings().values()) {
                String version = indexSettings.get(IndexMetadata.SETTING_VERSION_CREATED);
                if (version != null) {
                    versions.add(version);
                }
            }
            onVersions.accept(List.copyOf(versions));
        }));
    }

    /**
     * A listener that hands {@code null} to {@code onResponse} instead of failing, for a call whose result is a nice-to-have.
     */
    private static <T> ActionListener<T> withoutFailing(Logger logger, String what, Consumer<T> onResponse) {
        return new ActionListener<>() {
            @Override
            public void onResponse(T response) {
                onResponse.accept(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug(() -> "could not read " + what + "; omitting it from the reported environment", e);
                onResponse.accept(null);
            }
        };
    }

    @Nullable
    private static KnnEvalEnvironment.IndexSummary indexSummary(@Nullable IndicesSegmentResponse response) {
        if (response == null) {
            return null;
        }
        List<Long> segmentDocs = new ArrayList<>();
        long liveDocs = 0;
        long deletedDocs = 0;
        long storeSize = 0;
        int shards = 0;
        for (IndexSegments indexSegments : response.getIndices().values()) {
            for (IndexShardSegments shardSegments : indexSegments) {
                for (ShardSegments shard : shardSegments) {
                    // primaries only: a replica's segments are the same documents counted twice
                    if (shard.getShardRouting().primary() == false) {
                        continue;
                    }
                    shards++;
                    for (Segment segment : shard) {
                        segmentDocs.add((long) segment.getNumDocs());
                        liveDocs += segment.getNumDocs();
                        deletedDocs += segment.getDeletedDocs();
                        storeSize += segment.getSize() == null ? 0 : segment.getSize().getBytes();
                    }
                }
            }
        }
        Collections.sort(segmentDocs);
        int count = segmentDocs.size();
        return new KnnEvalEnvironment.IndexSummary(
            response.getIndices().size(),
            shards,
            liveDocs,
            deletedDocs,
            count,
            count == 0 ? 0 : segmentDocs.get(0),
            median(segmentDocs),
            count == 0 ? 0 : segmentDocs.get(count - 1),
            storeSize
        );
    }

    private static long median(List<Long> sorted) {
        if (sorted.isEmpty()) {
            return 0;
        }
        int middle = sorted.size() / 2;
        return sorted.size() % 2 == 1 ? sorted.get(middle) : (sorted.get(middle - 1) + sorted.get(middle)) / 2;
    }
}
