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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.regex.Pattern;

import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Operation;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.StatsTracker;

final class GoogleCloudStorageHttpStatsCollector implements HttpResponseInterceptor {

    private static final Logger logger = LogManager.getLogger("GcpHttpStats");

    private final StatsTracker stats;
    private final OperationPurpose purpose;
    private final Pattern getObjPattern;
    private final Pattern insertObjPattern;
    private final Pattern listObjPattern;

    GoogleCloudStorageHttpStatsCollector(final GoogleCloudStorageOperationsStats stats, OperationPurpose purpose) {
        this.stats = stats.tracker();
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
        stats.trackOperationAndRequest(purpose, operation);
    }

    @Override
    public void interceptResponse(final HttpResponse response) {
        var respCode = response.getStatusCode();
        // Some of the intermediate and error codes are still counted as "good" requests
        if (((respCode >= 200 && respCode < 300) || respCode == 308 || respCode == 404) == false) {
            return;
        }
        var request = response.getRequest();

        var path = request.getUrl().getRawPath();
        var ignored = false;
        switch (request.getRequestMethod()) {
            case "GET" -> {
                // https://cloud.google.com/storage/docs/json_api/v1/objects/get
                if (getObjPattern.matcher(path).matches()) {
                    trackRequestAndOperation(Operation.GET_OBJECT);
                } else if (listObjPattern.matcher(path).matches()) {
                    trackRequestAndOperation(Operation.LIST_OBJECTS);
                } else {
                    ignored = true;
                }
            }
            case "POST", "PUT" -> {
                // https://cloud.google.com/storage/docs/json_api/v1/objects/insert
                if (insertObjPattern.matcher(path).matches()) {
                    trackRequest(Operation.INSERT_OBJECT);
                } else {
                    ignored = true;
                }
            }
            default -> ignored = true;
        }
        if (ignored) {
            logger.debug(
                "not handling request:{} {} response:{} {}",
                request.getRequestMethod(),
                path,
                response.getStatusCode(),
                response.getStatusMessage()
            );
        }
    }
}
