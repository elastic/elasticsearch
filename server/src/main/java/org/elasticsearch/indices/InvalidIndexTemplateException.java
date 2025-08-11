/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class InvalidIndexTemplateException extends ElasticsearchException {

    private final String name;

    public InvalidIndexTemplateException(String name, String msg) {
        super("index_template [" + name + "] invalid, cause [" + msg + "]");
        this.name = name;
    }

    public String name() {
        return this.name;
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeOptionalString(name);
    }

    public InvalidIndexTemplateException(StreamInput in) throws IOException {
        super(in);
        name = in.readOptionalString();
    }
}
