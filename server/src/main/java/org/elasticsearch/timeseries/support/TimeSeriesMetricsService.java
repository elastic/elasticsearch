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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.TimeSeriesParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Internal service for querying and accessing time series by language that creates an abstraction layer around legacy
 * metric indices and times series indices.
 */
public class TimeSeriesMetricsService {
    private final Client client;
    private final int bucketBatchSize;
    private final int docBatchSize;
    private final TimeValue staleness;

    public TimeSeriesMetricsService(Client client, int bucketBatchSize, int docBatchSize, TimeValue staleness) {
        // TODO read maxBuckets at runtime
        this.client = client;
        this.bucketBatchSize = bucketBatchSize;
        this.docBatchSize = docBatchSize;
        this.staleness = staleness;
    }

    /**
     * Returns an interface through which it will be possible to perform multiple time series queries.
     *
     * @param indices - a list of indices for the query
     * @param indicesOptions - options of resolving indices
     * @param listener - the result listener
     */
    public void newMetrics(String[] indices, IndicesOptions indicesOptions, ActionListener<TimeSeriesMetrics> listener) {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.indices(indices);
        request.fields("*");
        request.indicesOptions(indicesOptions);
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
        Map<String, TimeSeriesParams.MetricType> metricFieldNames = new TreeMap<>();
        for (Map.Entry<String, Map<String, FieldCapabilities>> e : response.get().entrySet()) {
            for (Map.Entry<String, FieldCapabilities> e2 : e.getValue().entrySet()) {
                collectField(dimensionFieldNames, metricFieldNames, e.getKey(), e2.getKey(), e2.getValue());
            }
        }
        return new TimeSeriesMetrics(
            bucketBatchSize,
            docBatchSize,
            staleness,
            client,
            response.getIndices(),
            dimensionFieldNames,
            metricFieldNames
        );
    }

    private static void collectField(
        List<String> dimensions,
        Map<String, TimeSeriesParams.MetricType> metrics,
        String fieldName,
        String fieldType,
        FieldCapabilities capabilities
    ) {
        if (capabilities.isDimension()) {
            dimensions.add(fieldName);
        } else {
            if (capabilities.getMetricType() != null) {
                metrics.put(fieldName, capabilities.getMetricType());
            }
        }
    }
}
