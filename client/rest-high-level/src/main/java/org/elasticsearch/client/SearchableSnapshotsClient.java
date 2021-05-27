/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.searchable_snapshots.CachesStatsRequest;
import org.elasticsearch.client.searchable_snapshots.CachesStatsResponse;
import org.elasticsearch.client.searchable_snapshots.MountSnapshotRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for accessing searchable snapshots APIs.
 *
 * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/searchable-snapshots-apis.html">Searchable Snapshots
 * APIs on elastic.co</a> for more information.
 */
public class SearchableSnapshotsClient {

    private RestHighLevelClient restHighLevelClient;

    public SearchableSnapshotsClient(final RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = Objects.requireNonNull(restHighLevelClient);
    }

    /**
     * Executes the mount snapshot API, which mounts a snapshot as a searchable snapshot.
     *
     *  See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/searchable-snapshots-api-mount-snapshot.html"> the
     *  docs</a> for more information.
     *
     * @param request the request
     * @param options the request options
     * @return the response
     * @throws IOException if an I/O exception occurred sending the request, or receiving or parsing the response
     */
    public RestoreSnapshotResponse mountSnapshot(final MountSnapshotRequest request, final RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            SearchableSnapshotsRequestConverters::mountSnapshot,
            options,
            RestoreSnapshotResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously executes the mount snapshot API, which mounts a snapshot as a searchable snapshot.
     *
     * @param request the request
     * @param options the request options
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable mountSnapshotAsync(
        final MountSnapshotRequest request,
        final RequestOptions options,
        final ActionListener<RestoreSnapshotResponse> listener)
    {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            SearchableSnapshotsRequestConverters::mountSnapshot,
            options,
            RestoreSnapshotResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Executes the cache stats API, which provides statistics about searchable snapshot cache.
     *
     *  See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/searchable-snapshots-api-cache-stats.html"> the
     *  docs</a> for more information.
     *
     * @param request the request
     * @param options the request options
     * @return the response
     * @throws IOException if an I/O exception occurred sending the request, or receiving or parsing the response
     */
    public CachesStatsResponse cacheStats(final CachesStatsRequest request, final RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            SearchableSnapshotsRequestConverters::cacheStats,
            options,
            CachesStatsResponse::fromXContent,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously executes the cache stats API, which provides statistics about searchable snapshot cache.
     *
     * @param request the request
     * @param options the request options
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable cacheStatsAsync(
        final CachesStatsRequest request,
        final RequestOptions options,
        final ActionListener<CachesStatsResponse> listener)
    {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            SearchableSnapshotsRequestConverters::cacheStats,
            options,
            CachesStatsResponse::fromXContent,
            listener,
            Collections.emptySet()
        );
    }
}
