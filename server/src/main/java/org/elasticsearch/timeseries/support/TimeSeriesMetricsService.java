/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.timeseries.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.client.Client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TimeSeriesMetricsService {
    private final Client client;
    private final int bucketBatchSize;
    private final int docBatchSize;

    public TimeSeriesMetricsService(Client client, int bucketBatchSize, int docBatchSize) { // TODO read maxBuckets at runtime
        this.client = client;
        this.bucketBatchSize = bucketBatchSize;
        this.docBatchSize = docBatchSize;
    }

    public void newMetrics(String[] indices, ActionListener<TimeSeriesMetrics> listener) {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.indices(indices);
        request.fields("*"); // TODO * can be a lot!
        client.fieldCaps(request, listener.map(this::newMetrics));
    }

    private TimeSeriesMetrics newMetrics(FieldCapabilitiesResponse response) {
        if (response.getFailures().isEmpty() == false) {
            ElasticsearchException e = new ElasticsearchException(
                "Failed to fetch field caps for " + Arrays.toString(response.getFailedIndices())
            );
            for (FieldCapabilitiesFailure f : response.getFailures()) {
                e.addSuppressed(
                    new ElasticsearchException("Failed to fetch field caps for " + Arrays.toString(f.getIndices()), f.getException())
                );
            }
            throw e;
        }
        List<String> dimensionFieldNames = new ArrayList<>();
        for (Map.Entry<String, Map<String, FieldCapabilities>> e : response.get().entrySet()) {
            for (Map.Entry<String, FieldCapabilities> e2 : e.getValue().entrySet()) {
                collectField(dimensionFieldNames, e.getKey(), e2.getKey(), e2.getValue());
            }
        }
        return new TimeSeriesMetrics(bucketBatchSize, docBatchSize, client, response.getIndices(), List.copyOf(dimensionFieldNames));
    }

    private void collectField(List<String> dimensions, String fieldName, String fieldType, FieldCapabilities capabilities) {
        // TODO collect metrics for selector
        if (capabilities.isDimension()) {
            dimensions.add(fieldName);
        }
    }
}
