/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * A generic exception indicating failure to generate.
 *
 *
 */
public class ElasticsearchGenerationException extends ElasticsearchException {

    public ElasticsearchGenerationException(String msg) {
        super(msg);
    }

    public ElasticsearchGenerationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ElasticsearchGenerationException(StreamInput in) throws IOException {
        super(in);
    }
}
