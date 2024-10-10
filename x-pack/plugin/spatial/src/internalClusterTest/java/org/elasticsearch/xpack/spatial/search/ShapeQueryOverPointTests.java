/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

public class ShapeQueryOverPointTests extends ShapeQueryTestCase {
    @Override
    protected XContentBuilder createDefaultMapping() throws Exception {
        final boolean isIndexed = randomBoolean();
        final boolean hasDocValues = isIndexed == false || randomBoolean();
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(defaultFieldName)
            .field("type", "point")
            .field("index", isIndexed)
            .field("doc_values", hasDocValues)
            .endObject()
            .endObject()
            .endObject();
    }
}
