/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StringStoredFieldFieldLoaderTests extends ESTestCase {

    public void testLoadStoredFieldAndReset() throws IOException {
        var sfl = new StringStoredFieldFieldLoader("foo", "foo") {
            @Override
            protected void write(XContentBuilder b, Object value) throws IOException {
                b.value((String) value);
            }
        };

        var storedFieldLoaders = sfl.storedFieldLoaders().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        storedFieldLoaders.get("foo").load(List.of("one"));

        var result = XContentBuilder.builder(XContentType.JSON.xContent());
        result.startObject();
        sfl.write(result);
        result.endObject();

        assertEquals("""
            {"foo":"one"}""", Strings.toString(result));

        var empty = XContentBuilder.builder(XContentType.JSON.xContent());
        empty.startObject();
        // reset() should have been called after previous write
        sfl.write(empty);
        empty.endObject();

        assertEquals("{}", Strings.toString(empty));
    }

}
