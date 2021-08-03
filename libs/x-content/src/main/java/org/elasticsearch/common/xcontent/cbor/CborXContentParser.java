/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.cbor;

import com.fasterxml.jackson.core.JsonParser;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;

public class CborXContentParser extends JsonXContentParser {

    public CborXContentParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, JsonParser parser) {
        super(xContentRegistry, deprecationHandler, parser);
    }

    public CborXContentParser(NamedXContentRegistry xContentRegistry,
                              DeprecationHandler deprecationHandler, JsonParser parser,
                              RestApiVersion restApiVersion) {
        super(xContentRegistry, deprecationHandler, parser, restApiVersion);
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
