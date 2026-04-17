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
import java.util.stream.Stream;

/**
 * Thrown when ES|QL detects views during cross-cluster search field resolution.
 * Views are not supported in CCS and the query must fail.
 */
public class RemoteViewNotSupportedException extends ElasticsearchException {

    private static final String VIEW_NAMES_KEY = "es.esql.view.names";

    @SuppressWarnings("this-escape")
    public RemoteViewNotSupportedException(List<String> views) {
        super(message(views));
        addMetadata(VIEW_NAMES_KEY, views);
    }

    public RemoteViewNotSupportedException(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Merge two exceptions into one that reports all matched views.
     */
    public static RemoteViewNotSupportedException merge(RemoteViewNotSupportedException a, RemoteViewNotSupportedException b) {
        return new RemoteViewNotSupportedException(
            Stream.concat(a.getMetadata(VIEW_NAMES_KEY).stream(), b.getMetadata(VIEW_NAMES_KEY).stream()).toList()
        );
    }

    private static String message(List<String> views) {
        String exclusions = views.stream().map(v -> {
            String[] clusterAndIndex = RemoteClusterAware.splitIndexName(v);
            return clusterAndIndex[0] + ":-" + clusterAndIndex[1];
        }).collect(Collectors.joining(","));
        return "ES|QL queries with remote views are not supported. Matched "
            + views
            + ". Remove them from the query pattern or exclude them with ["
            + exclusions
            + "] if matched by a wildcard.";
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
