/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.indices;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class IndexTemplateMissingException extends ElasticsearchException {

    private final String name;

    public IndexTemplateMissingException(String name) {
        super("index_template [" + name + "] missing");
        this.name = name;
    }

    public IndexTemplateMissingException(StreamInput in) throws IOException {
        super(in);
        name = in.readOptionalString();
    }

    public String name() {
        return this.name;
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeOptionalString(name);
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }
}
