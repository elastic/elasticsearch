/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for accessing the Snapshot API.
 * <p>
 * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html">Snapshot API on elastic.co</a>
 */
public final class SnapshotClient {
    private final RestHighLevelClient restHighLevelClient;

    SnapshotClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Gets a list of snapshot repositories. If the list of repositories is empty or it contains a single element "_all", all
     * registered repositories are returned.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param getRepositoriesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetRepositoriesResponse getRepository(GetRepositoriesRequest getRepositoriesRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(getRepositoriesRequest, SnapshotRequestConverters::getRepositories, options,
            GetRepositoriesResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously gets a list of snapshot repositories. If the list of repositories is empty or it contains a single element "_all", all
     * registered repositories are returned.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param getRepositoriesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getRepositoryAsync(GetRepositoriesRequest getRepositoriesRequest, RequestOptions options,
                                          ActionListener<GetRepositoriesResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(getRepositoriesRequest,
            SnapshotRequestConverters::getRepositories, options,
            GetRepositoriesResponse::fromXContent, listener, emptySet());
    }

    /**
     * Creates a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param putRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse createRepository(PutRepositoryRequest putRepositoryRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(putRepositoryRequest, SnapshotRequestConverters::createRepository, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously creates a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param putRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable createRepositoryAsync(PutRepositoryRequest putRepositoryRequest, RequestOptions options,
                                             ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(putRepositoryRequest,
            SnapshotRequestConverters::createRepository, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Deletes a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param deleteRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deleteRepository(DeleteRepositoryRequest deleteRepositoryRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(deleteRepositoryRequest, SnapshotRequestConverters::deleteRepository,
            options, AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously deletes a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param deleteRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteRepositoryAsync(DeleteRepositoryRequest deleteRepositoryRequest, RequestOptions options,
                                             ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(deleteRepositoryRequest,
            SnapshotRequestConverters::deleteRepository, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Verifies a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param verifyRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public VerifyRepositoryResponse verifyRepository(VerifyRepositoryRequest verifyRepositoryRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(verifyRepositoryRequest, SnapshotRequestConverters::verifyRepository,
            options, VerifyRepositoryResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously verifies a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param verifyRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable verifyRepositoryAsync(VerifyRepositoryRequest verifyRepositoryRequest, RequestOptions options,
                                             ActionListener<VerifyRepositoryResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(verifyRepositoryRequest,
            SnapshotRequestConverters::verifyRepository, options,
            VerifyRepositoryResponse::fromXContent, listener, emptySet());
    }

    /**
     * Cleans up a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param cleanupRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public CleanupRepositoryResponse cleanupRepository(CleanupRepositoryRequest cleanupRepositoryRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(cleanupRepositoryRequest, SnapshotRequestConverters::cleanupRepository,
            options, CleanupRepositoryResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously cleans up a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param cleanupRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable cleanupRepositoryAsync(CleanupRepositoryRequest cleanupRepositoryRequest, RequestOptions options,
                                       ActionListener<CleanupRepositoryResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(cleanupRepositoryRequest, SnapshotRequestConverters::cleanupRepository,
            options, CleanupRepositoryResponse::fromXContent, listener, emptySet());
    }

    /**
     * Creates a snapshot.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     */
    public CreateSnapshotResponse create(CreateSnapshotRequest createSnapshotRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(createSnapshotRequest, SnapshotRequestConverters::createSnapshot, options,
            CreateSnapshotResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously creates a snapshot.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable createAsync(CreateSnapshotRequest createSnapshotRequest, RequestOptions options,
                                   ActionListener<CreateSnapshotResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(createSnapshotRequest,
            SnapshotRequestConverters::createSnapshot, options,
            CreateSnapshotResponse::fromXContent, listener, emptySet());
    }

    /**
     * Clones a snapshot.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     */
    public AcknowledgedResponse clone(CloneSnapshotRequest cloneSnapshotRequest, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(cloneSnapshotRequest, SnapshotRequestConverters::cloneSnapshot, options,
                AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously clones a snapshot.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable cloneAsync(CloneSnapshotRequest cloneSnapshotRequest, RequestOptions options,
                                   ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(cloneSnapshotRequest,
                SnapshotRequestConverters::cloneSnapshot, options,
                AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Get snapshots.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     *
     * @param getSnapshotsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetSnapshotsResponse get(GetSnapshotsRequest getSnapshotsRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(getSnapshotsRequest, SnapshotRequestConverters::getSnapshots, options,
            GetSnapshotsResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously get snapshots.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     *  @param getSnapshotsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getAsync(GetSnapshotsRequest getSnapshotsRequest, RequestOptions options,
                                ActionListener<GetSnapshotsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(getSnapshotsRequest,
            SnapshotRequestConverters::getSnapshots, options,
            GetSnapshotsResponse::fromXContent, listener, emptySet());
    }

    /**
     * Gets the status of requested snapshots.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param snapshotsStatusRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public SnapshotsStatusResponse status(SnapshotsStatusRequest snapshotsStatusRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(snapshotsStatusRequest, SnapshotRequestConverters::snapshotsStatus, options,
            SnapshotsStatusResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously gets the status of requested snapshots.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param snapshotsStatusRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable statusAsync(SnapshotsStatusRequest snapshotsStatusRequest, RequestOptions options,
                                   ActionListener<SnapshotsStatusResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(snapshotsStatusRequest,
            SnapshotRequestConverters::snapshotsStatus, options,
            SnapshotsStatusResponse::fromXContent, listener, emptySet());
    }

    /**
     * Restores a snapshot.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     *
     * @param restoreSnapshotRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public RestoreSnapshotResponse restore(RestoreSnapshotRequest restoreSnapshotRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(restoreSnapshotRequest, SnapshotRequestConverters::restoreSnapshot, options,
            RestoreSnapshotResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously restores a snapshot.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     *
     * @param restoreSnapshotRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable restoreAsync(RestoreSnapshotRequest restoreSnapshotRequest, RequestOptions options,
                                    ActionListener<RestoreSnapshotResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(restoreSnapshotRequest,
            SnapshotRequestConverters::restoreSnapshot, options,
            RestoreSnapshotResponse::fromXContent, listener, emptySet());
    }

    /**
     * Deletes a snapshot.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     *
     * @param deleteSnapshotRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse delete(DeleteSnapshotRequest deleteSnapshotRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(deleteSnapshotRequest,
            SnapshotRequestConverters::deleteSnapshot, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously deletes a snapshot.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     *
     * @param deleteSnapshotRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteAsync(DeleteSnapshotRequest deleteSnapshotRequest, RequestOptions options,
                                   ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(deleteSnapshotRequest,
            SnapshotRequestConverters::deleteSnapshot, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
    }
}
