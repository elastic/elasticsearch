/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.VerificationException;

/**
 * A query named a dataset whose data source an admin closed. It is a client error (400). A closed index fails a query
 * through a cluster block (403); a dataset has no block, so this is an error of its own. It is a type of its own so failure
 * telemetry can file it apart from a user's query mistakes and from {@code other}, and the message names the dataset only:
 * reading a dataset needs {@code read} on the dataset, so the caller may not be allowed to learn the name of its data source.
 * <p>
 * The same refusal is raised by a data node that starts its part of a query after the close ({@link #refusedByDataNode}). It
 * cannot be this class: it must stay an {@link ElasticsearchStatusException} with status 400, which the data node handler
 * reads as "possibly a node that is behind". A class does not survive the trip back to the coordinator either, so a metadata
 * entry marks it, and {@link #isClosedRefusal} recognizes both.
 */
public final class DatasetClosedException extends VerificationException {

    /** Metadata that marks a closed-dataset refusal raised on a data node; metadata crosses a node boundary, the class does not. */
    public static final String DATA_NODE_REFUSAL_METADATA_KEY = "es.dataset_closed";

    public DatasetClosedException(String dataset) {
        super("dataset [{}] is closed", dataset);
    }

    /** The refusal of a data node, for a dataset of a data source that is closed on its cluster state. */
    public static ElasticsearchStatusException refusedByDataNode(String dataset) {
        ElasticsearchStatusException refusal = new ElasticsearchStatusException(
            "external dataset [{}] is closed",
            RestStatus.BAD_REQUEST,
            dataset
        );
        refusal.addMetadata(DATA_NODE_REFUSAL_METADATA_KEY, dataset);
        return refusal;
    }

    /** Whether {@code t} is a closed-dataset refusal, from the coordinator or, after crossing a node boundary, from a data node. */
    public static boolean isClosedRefusal(Throwable t) {
        return t instanceof DatasetClosedException
            || (t instanceof ElasticsearchException e && e.getMetadata(DATA_NODE_REFUSAL_METADATA_KEY) != null);
    }
}
