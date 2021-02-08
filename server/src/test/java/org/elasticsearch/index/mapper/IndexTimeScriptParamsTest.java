/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class IndexTimeScriptParamsTest extends ESTestCase {

    public void testSource() throws IOException {
        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .field("foo", "bar")
            .endObject());

        IndexTimeScriptParams params = new IndexTimeScriptParams(source, f -> null);
        assertEquals("bar", params.source().get("foo"));
    }

    public void testDoc() throws IOException {
        MappedFieldType longField = new NumberFieldMapper.NumberFieldType("longfield", NumberFieldMapper.NumberType.LONG);

        IndexTimeScriptParams params = new IndexTimeScriptParams(
            new BytesArray("{\"longfield\":[10, 20]}"),
            f -> longField);
        ScriptDocValues<?> values = params.doc().get("longfield");
        assertEquals(10, values.get(0));
        assertEquals(20, values.get(0));
        assertEquals(2, values.size());
    }
}
