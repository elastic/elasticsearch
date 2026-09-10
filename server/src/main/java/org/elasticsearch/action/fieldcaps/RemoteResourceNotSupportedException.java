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
 * Thrown by the coordinator when ES|QL resolves a query to a remote view under cross-cluster or cross-project search.
 * A view is a non-remotable abstraction and the query must fail.
 * <p>
 * Remote nodes report views via {@link RemoteViewNotSupportedException}; the coordinator aggregates the ones matched
 * across several clusters into this single exception, so a query reaching a view on more than one of them names all of
 * them at once rather than surfacing whichever check ran first. The message reuses the per-kind wording verbatim.
 * <p>
 * The dataset half is vestigial. It was carried when a dataset on another cluster failed a query too; a dataset is now
 * invisible across a cluster boundary, so nothing populates the list. The shape stays because changing what is on the
 * wire is not free and there is nothing to gain by it here. The views half is what still has a producer: cross-project
 * search rejects a remote view, and so does a cluster asked to resolve views by a coordinator old enough to ask.
 */
public class RemoteResourceNotSupportedException extends ElasticsearchException {

    private static final String VIEW_NAMES_KEY = "es.esql.view.names";
    private static final String DATASET_NAMES_KEY = "es.esql.dataset.names";

    @SuppressWarnings("this-escape")
    public RemoteResourceNotSupportedException(List<String> views, List<String> datasets) {
        super(message(views, datasets));
        if (views.isEmpty() == false) {
            addMetadata(VIEW_NAMES_KEY, views);
        }
        if (datasets.isEmpty() == false) {
            addMetadata(DATASET_NAMES_KEY, datasets);
        }
    }

    public RemoteResourceNotSupportedException(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * The qualified names of the remote views that triggered this exception, or an empty list if it was datasets only.
     */
    public List<String> views() {
        List<String> views = getMetadata(VIEW_NAMES_KEY);
        return views == null ? List.of() : views;
    }

    /**
     * The qualified names of the remote datasets that triggered this exception, or an empty list if it was views only.
     */
    public List<String> datasets() {
        List<String> datasets = getMetadata(DATASET_NAMES_KEY);
        return datasets == null ? List.of() : datasets;
    }

    private static String message(List<String> views, List<String> datasets) {
        boolean hasViews = views.isEmpty() == false;
        boolean hasDatasets = datasets.isEmpty() == false;
        assert hasViews || hasDatasets : "RemoteResourceNotSupportedException requires at least one unsupported resource";

        String kinds;
        String matched;
        String exclusions;
        if (hasViews && hasDatasets) {
            kinds = "views and datasets";
            matched = "views " + views + ", datasets " + datasets;
            exclusions = exclusionsOf(views) + "," + exclusionsOf(datasets);
        } else if (hasViews) {
            kinds = "views";
            matched = views.toString();
            exclusions = exclusionsOf(views);
        } else {
            kinds = "datasets";
            matched = datasets.toString();
            exclusions = exclusionsOf(datasets);
        }
        return "ES|QL queries with remote "
            + kinds
            + " are not supported. Matched "
            + matched
            + ". Remove them from the query pattern or exclude them with ["
            + exclusions
            + "] if matched by a wildcard.";
    }

    private static String exclusionsOf(List<String> names) {
        return names.stream().map(name -> {
            var clusterAndIndex = RemoteClusterAware.splitIndexName(name);
            return clusterAndIndex.clusterAlias() + ":-" + clusterAndIndex.indexExpression();
        }).collect(Collectors.joining(","));
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
