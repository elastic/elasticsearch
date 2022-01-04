/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.xcontent.cbor;

import com.fasterxml.jackson.core.JsonParser;

import org.elasticsearch.xpack.sql.proto.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.sql.proto.xcontent.XContentType;
import org.elasticsearch.xpack.sql.proto.xcontent.json.JsonXContentParser;

public class CborXContentParser extends JsonXContentParser {

    public CborXContentParser(XContentParserConfiguration config, JsonParser parser) {
        super(config, parser);
    }

    @Override
    public XContentType contentType() {
        return XContentType.CBOR;
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        throw new UnsupportedOperationException("Allowing duplicate keys after the parser has been created is not possible for CBOR");
    }
}
