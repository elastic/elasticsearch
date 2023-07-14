/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal.metering.serverless;

import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.xcontent.XContentParser;

public class CountingDocumentParserContext extends DocumentParserContext.Wrapper {
    long counter = 0L;

    public CountingDocumentParserContext(ObjectMapper parent, DocumentParserContext in) {
        super(parent, in);
    }

    @Override
    public XContentParser parser() {
        XContentParser parser = super.parser();
        return wrapParser(parser);
    }

    private XContentParser wrapParser(XContentParser parser) {
        MeteringParser meteringParser = new MeteringParser(parser);
        return meteringParser;
    }

    public long getCount() {
        return counter;
    }

}
