/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Thrown when ES|QL detects datasets during cross-cluster search field resolution.
 * Datasets are not supported in CCS and the query must fail.
 */
public class RemoteDatasetNotSupportedException extends ElasticsearchException {

    private static final String DATASET_NAMES_KEY = "es.esql.dataset.names";

    @SuppressWarnings("this-escape")
    public RemoteDatasetNotSupportedException(List<String> datasets) {
        super(message(datasets));
        addMetadata(DATASET_NAMES_KEY, datasets);
    }

    public RemoteDatasetNotSupportedException(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * The qualified names of the remote datasets that triggered this exception.
     */
    public List<String> datasets() {
        List<String> datasets = getMetadata(DATASET_NAMES_KEY);
        return datasets == null ? List.of() : datasets;
    }

    private static String message(List<String> datasets) {
        String exclusions = datasets.stream().map(v -> {
            var clusterAndIndex = RemoteClusterAware.splitIndexName(v);
            return clusterAndIndex.clusterAlias() + ":-" + clusterAndIndex.indexExpression();
        }).collect(Collectors.joining(","));
        return "ES|QL queries with remote datasets are not supported. Matched "
            + datasets
            + ". Remove them from the query pattern or exclude them with ["
            + exclusions
            + "] if matched by a wildcard.";
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
