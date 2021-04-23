/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.common.Strings;

import java.io.IOException;

final class SnapshotRequestConverters {

    private SnapshotRequestConverters() {}

    static Request getRepositories(GetRepositoriesRequest getRepositoriesRequest) {
        String[] repositories = getRepositoriesRequest.repositories() == null ? Strings.EMPTY_ARRAY : getRepositoriesRequest.repositories();
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_snapshot").addCommaSeparatedPathParts(repositories)
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(getRepositoriesRequest.masterNodeTimeout());
        parameters.withLocal(getRepositoriesRequest.local());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request createRepository(PutRepositoryRequest putRepositoryRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPart("_snapshot").addPathPart(putRepositoryRequest.name()).build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(putRepositoryRequest.masterNodeTimeout());
        parameters.withTimeout(putRepositoryRequest.timeout());
        if (putRepositoryRequest.verify() == false) {
            parameters.putParam("verify", "false");
        }
        request.addParameters(parameters.asMap());
        request.setEntity(RequestConverters.createEntity(putRepositoryRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request deleteRepository(DeleteRepositoryRequest deleteRepositoryRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_snapshot").addPathPart(deleteRepositoryRequest.name())
            .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(deleteRepositoryRequest.masterNodeTimeout());
        parameters.withTimeout(deleteRepositoryRequest.timeout());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request verifyRepository(VerifyRepositoryRequest verifyRepositoryRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_snapshot")
            .addPathPart(verifyRepositoryRequest.name())
            .addPathPartAsIs("_verify")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(verifyRepositoryRequest.masterNodeTimeout());
        parameters.withTimeout(verifyRepositoryRequest.timeout());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request cleanupRepository(CleanupRepositoryRequest cleanupRepositoryRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_snapshot")
            .addPathPart(cleanupRepositoryRequest.name())
            .addPathPartAsIs("_cleanup")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(cleanupRepositoryRequest.masterNodeTimeout());
        parameters.withTimeout(cleanupRepositoryRequest.timeout());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request createSnapshot(CreateSnapshotRequest createSnapshotRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPart("_snapshot")
            .addPathPart(createSnapshotRequest.repository())
            .addPathPart(createSnapshotRequest.snapshot())
            .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(createSnapshotRequest.masterNodeTimeout());
        params.withWaitForCompletion(createSnapshotRequest.waitForCompletion());
        request.addParameters(params.asMap());
        request.setEntity(RequestConverters.createEntity(createSnapshotRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request cloneSnapshot(CloneSnapshotRequest cloneSnapshotRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPart("_snapshot")
                .addPathPart(cloneSnapshotRequest.repository())
                .addPathPart(cloneSnapshotRequest.source())
                .addPathPart("_clone")
                .addPathPart(cloneSnapshotRequest.target())
                .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(cloneSnapshotRequest.masterNodeTimeout());
        request.addParameters(params.asMap());
        request.setEntity(RequestConverters.createEntity(cloneSnapshotRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getSnapshots(GetSnapshotsRequest getSnapshotsRequest) {
        RequestConverters.EndpointBuilder endpointBuilder = new RequestConverters.EndpointBuilder().addPathPartAsIs("_snapshot")
            .addCommaSeparatedPathParts(getSnapshotsRequest.repositories());
        String endpoint;
        if (getSnapshotsRequest.snapshots().length == 0) {
            endpoint = endpointBuilder.addPathPart("_all").build();
        } else {
            endpoint = endpointBuilder.addCommaSeparatedPathParts(getSnapshotsRequest.snapshots()).build();
        }

        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(getSnapshotsRequest.masterNodeTimeout());
        parameters.putParam("ignore_unavailable", Boolean.toString(getSnapshotsRequest.ignoreUnavailable()));
        parameters.putParam("verbose", Boolean.toString(getSnapshotsRequest.verbose()));
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request snapshotsStatus(SnapshotsStatusRequest snapshotsStatusRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_snapshot")
            .addPathPart(snapshotsStatusRequest.repository())
            .addCommaSeparatedPathParts(snapshotsStatusRequest.snapshots())
            .addPathPartAsIs("_status")
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(snapshotsStatusRequest.masterNodeTimeout());
        parameters.withIgnoreUnavailable(snapshotsStatusRequest.ignoreUnavailable());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request restoreSnapshot(RestoreSnapshotRequest restoreSnapshotRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_snapshot")
            .addPathPart(restoreSnapshotRequest.repository())
            .addPathPart(restoreSnapshotRequest.snapshot())
            .addPathPartAsIs("_restore")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(restoreSnapshotRequest.masterNodeTimeout());
        parameters.withWaitForCompletion(restoreSnapshotRequest.waitForCompletion());
        request.addParameters(parameters.asMap());
        request.setEntity(RequestConverters.createEntity(restoreSnapshotRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request deleteSnapshot(DeleteSnapshotRequest deleteSnapshotRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_snapshot")
            .addPathPart(deleteSnapshotRequest.repository())
            .addCommaSeparatedPathParts(deleteSnapshotRequest.snapshots())
            .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(deleteSnapshotRequest.masterNodeTimeout());
        request.addParameters(parameters.asMap());
        return request;
    }
}
