/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseInterceptor;

import org.elasticsearch.common.blobstore.OperationPurpose;

import java.util.regex.Pattern;

import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Operation;

final class GoogleCloudStorageHttpStatsCollector implements HttpResponseInterceptor {
    private final GoogleCloudStorageOperationsStats stats;
    private final OperationPurpose purpose;
    private final Pattern getObjPattern;
    private final Pattern insertObjPattern;
    private final Pattern listObjPattern;

    GoogleCloudStorageHttpStatsCollector(final GoogleCloudStorageOperationsStats stats, OperationPurpose purpose) {
        this.stats = stats;
        this.purpose = purpose;
        var bucket = stats.bucketName();

        // The specification for the current API (v1) endpoints can be found at:
        // https://cloud.google.com/storage/docs/json_api/v1
        this.getObjPattern = Pattern.compile("(/download)?/storage/v1/b/" + bucket + "/o/.+");
        this.insertObjPattern = Pattern.compile("(/upload)?/storage/v1/b/" + bucket + "/o");
        this.listObjPattern = Pattern.compile("/storage/v1/b/" + bucket + "/o");
    }

    private void trackRequest(Operation operation) {
        stats.trackRequest(purpose, operation);
    }

    private void trackRequestAndOperation(Operation operation) {
        stats.trackRequestAndOperation(purpose, operation);
    }

    @Override
    public void interceptResponse(final HttpResponse response) {
        var request = response.getRequest();
        var path = request.getUrl().getRawPath();
        var ignored = false;
        switch (request.getRequestMethod()) {
            case "GET" -> {
                // https://cloud.google.com/storage/docs/json_api/v1/objects/get
                if (getObjPattern.matcher(path).matches()) {
                    // Retrieves object metadata. When alt=media is included as a query parameter, retrieves object data.
                    if ("media".equals(request.getUrl().getFirst("alt"))) {
                        trackRequestAndOperation(Operation.GET_OBJECT);
                    } else {
                        trackRequestAndOperation(Operation.GET_METADATA);
                    }
                } else if (listObjPattern.matcher(path).matches()) {
                    trackRequestAndOperation(Operation.LIST_OBJECTS);
                } else {
                    ignored = true;
                }
            }
            case "POST", "PUT" -> {
                // https://cloud.google.com/storage/docs/json_api/v1/objects/insert
                if (insertObjPattern.matcher(path).matches()) {
                    var obj = request.getUrl().getFirst("uploadType");
                    if (obj instanceof String uploadType) {
                        switch (uploadType) {
                            // We dont track insert operations here, only requests. The reason is billing impact.
                            // Any insert, including multipart or resumable parts, are counted as one operation.
                            case "multipart" -> trackRequest(Operation.MULTIPART_UPLOAD);
                            case "resumable" -> trackRequest(Operation.RESUMABLE_UPLOAD);
                            default -> ignored = true;
                        }
                    } else {
                        ignored = true;
                    }
                } else {
                    ignored = true;
                }
            }
            default -> ignored = true;
        }
        assert ignored == false : "must handle response: " + request.getRequestMethod() + " " + path;
    }
}
