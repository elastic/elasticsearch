/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.smile;

import com.fasterxml.jackson.core.JsonGenerator;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContentGenerator;

import java.io.OutputStream;
import java.util.Set;

public class SmileXContentGenerator extends JsonXContentGenerator {

    public SmileXContentGenerator(JsonGenerator jsonGenerator, OutputStream os, Set<String> includes, Set<String> excludes) {
        super(jsonGenerator, os, includes, excludes);
    }

    @Override
    public XContentType contentType() {
        return XContentType.SMILE;
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
