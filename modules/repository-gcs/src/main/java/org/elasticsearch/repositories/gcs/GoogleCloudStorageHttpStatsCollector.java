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

import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Counter.OPERATION;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Counter.OPERATION_EXCEPTION;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Counter.RANGE_NOT_SATISFIED;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Counter.REQUEST;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Counter.REQUEST_EXCEPTION;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Counter.THROTTLE;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Operation;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Operation.GET_OBJECT;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Operation.INSERT_OBJECT;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Operation.LIST_OBJECTS;

final class GoogleCloudStorageHttpStatsCollector implements HttpResponseInterceptor {

    private static final Logger logger = LogManager.getLogger("GcpHttpStats");

    private final GoogleCloudStorageOperationsStats stats;
    private final OperationPurpose purpose;
    private final Pattern getObjPattern;
    private final Pattern insertObjPattern;
    private final Pattern listObjPattern;

    GoogleCloudStorageHttpStatsCollector(final GoogleCloudStorageOperationsStats stats, OperationPurpose purpose) {
        this.purpose = purpose;
        this.stats = stats;
        // The specification for the current API (v1) endpoints can be found at:
        // https://cloud.google.com/storage/docs/json_api/v1
        var bucket = stats.bucket();
        this.getObjPattern = Pattern.compile("(/download)?/storage/v1/b/" + bucket + "/o/.+");
        this.insertObjPattern = Pattern.compile("(/upload)?/storage/v1/b/" + bucket + "/o");
        this.listObjPattern = Pattern.compile("/storage/v1/b/" + bucket + "/o");
    }

    private void inc(Operation op, GoogleCloudStorageOperationsStats.Counter counter) {
        stats.inc(purpose, op, counter);
    }

    private boolean isSuccessCode(int code) {
        return (code >= 200 && code < 300) || code == 308;
    }

    @Override
    public void interceptResponse(final HttpResponse response) {
        var request = response.getRequest();
        var code = response.getStatusCode();
        var path = request.getUrl().getRawPath();
        var ignored = false;
        switch (request.getRequestMethod()) {
            case "GET" -> {
                // https://cloud.google.com/storage/docs/json_api/v1/objects/get
                if (getObjPattern.matcher(path).matches()) {
                    inc(GET_OBJECT, REQUEST);
                    inc(GET_OBJECT, OPERATION);
                    if (isSuccessCode(code) == false) {
                        inc(GET_OBJECT, REQUEST_EXCEPTION);
                        inc(GET_OBJECT, OPERATION_EXCEPTION);
                        if (code == 429) {
                            inc(GET_OBJECT, THROTTLE);
                        } else if (code == 416) {
                            inc(GET_OBJECT, RANGE_NOT_SATISFIED);
                        }
                    }
                } else if (listObjPattern.matcher(path).matches()) {
                    inc(LIST_OBJECTS, REQUEST);
                    inc(LIST_OBJECTS, OPERATION);
                    if (isSuccessCode(code) == false) {
                        inc(LIST_OBJECTS, REQUEST_EXCEPTION);
                        inc(LIST_OBJECTS, OPERATION_EXCEPTION);
                        if (code == 429) {
                            inc(LIST_OBJECTS, THROTTLE);
                        }
                    }
                } else {
                    ignored = true;
                }
            }
            case "POST", "PUT" -> {
                // https://cloud.google.com/storage/docs/json_api/v1/objects/insert
                if (insertObjPattern.matcher(path).matches()) {
                    inc(INSERT_OBJECT, REQUEST);
                    if (isSuccessCode(code) == false) {
                        inc(INSERT_OBJECT, REQUEST_EXCEPTION);
                        if (code == 429) {
                            inc(INSERT_OBJECT, THROTTLE);
                        }
                    }
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
