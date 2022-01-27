/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class BulkRequestParserJsonTests extends AbstractBulkRequestParserTests {
    @Override
    protected XContentBuilder getBuilder() throws IOException {
        return XContentFactory.jsonBuilder();
    }

    @Override
    protected XContentType getXContentType() {
        return XContentType.JSON;
    }
}
