/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class IndexCreationException extends ElasticsearchException implements ElasticsearchWrapperException {

    public IndexCreationException(String index, Throwable cause) {
        super("failed to create index [{}]", cause, index);
        setIndex(index);
    }

    public IndexCreationException(StreamInput in) throws IOException {
        super(in);
    }
}
