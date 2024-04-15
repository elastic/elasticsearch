/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * This exception is thrown when there is something wrong with the structure of the graph (such as the graph of pipelines) to be applied
 * to a document. For example, this is thrown when there are cycles in the graph when cycles are not allowed.
 */
public class GraphStructureException extends ElasticsearchException {

    public GraphStructureException(String message) {
        super(message);
    }

    public GraphStructureException(StreamInput in) throws IOException {
        super(in);
    }
}
