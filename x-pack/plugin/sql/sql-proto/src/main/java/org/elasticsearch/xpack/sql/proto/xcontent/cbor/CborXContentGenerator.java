/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.xcontent.cbor;

import com.fasterxml.jackson.core.JsonGenerator;

import org.elasticsearch.xpack.sql.proto.xcontent.XContentType;
import org.elasticsearch.xpack.sql.proto.xcontent.json.JsonXContentGenerator;

import java.io.OutputStream;

public class CborXContentGenerator extends JsonXContentGenerator {

    public CborXContentGenerator(JsonGenerator jsonGenerator, OutputStream os) {
        super(jsonGenerator, os);
    }

    @Override
    public XContentType contentType() {
        return XContentType.CBOR;
    }

    @Override
    public void usePrintLineFeedAtEnd() {
        // nothing here
    }

    @Override
    protected boolean supportsRawWrites() {
        return false;
    }
}
