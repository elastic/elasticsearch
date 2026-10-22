/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class KeywordOffsetDocValuesLoaderTests extends OffsetDocValuesLoaderTestCase {

    public void testOffsetArray() throws Exception {
        verifyOffsets("{\"field\":[\"z\",\"x\",\"y\",\"c\",\"b\",\"a\"]}");
        verifyOffsets("{\"field\":[\"z\",null,\"y\",\"c\",null,\"a\"]}");
    }

    /**
     * When an array's encoded offsets exceed {@link IndexWriter#MAX_TERM_LENGTH} and some of its values were ignored
     * (here via {@code ignore_above}), the array cannot be moved to {@code _ignored_source} either: the entry would
     * suppress the separately stored ignored values at load time. Such documents are rejected, with a clearer and
     * earlier error than the one Lucene produces for an oversized sorted doc value.
     */
    public void testOversizedOffsetArrayWithIgnoredValuesIsRejected() throws Exception {
        int numSlots = IndexWriter.MAX_TERM_LENGTH + between(1, 500);
        List<Object> values = new ArrayList<>(numSlots);
        for (int i = 0; i < numSlots; i++) {
            values.add(randomValue());
        }
        // A value longer than ignore_above gets no offsets slot and is stored in the field's own fallback storage.
        String ignoredValue = randomAlphaOfLength(20);
        values.add(between(0, values.size() - 1), ignoredValue);

        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties").startObject("field");
        minimalMapping(mapping);
        mapping.field("ignore_above", 10);
        mapping.endObject().endObject().endObject().endObject();
        try (var mapperService = createMapperService(mapping)) {
            var mapper = mapperService.documentMapper();

            var exception = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field("field", values))));
            assertThat(exception.getCause().getMessage(), Matchers.containsString("encoded offsets of field [field.offsets]"));
            assertThat(exception.getCause().getMessage(), Matchers.containsString("because some values of [field] were ignored"));
        }
    }

    public void testOffsetNestedArray() throws Exception {
        verifyOffsets("{\"field\":[\"z\",[\"y\"],[\"c\"],null,\"a\"]}", "{\"field\":[\"z\",\"y\",\"c\",null,\"a\"]}");
        verifyOffsets(
            "{\"field\":[\"z\",[\"y\", [\"k\"]],[\"c\", [\"l\"]],null,\"a\"]}",
            "{\"field\":[\"z\",\"y\",\"k\",\"c\",\"l\",null,\"a\"]}"
        );
    }

    @Override
    protected String getFieldTypeName() {
        return "keyword";
    }

    @Override
    protected Object randomValue() {
        return randomAlphanumericOfLength(2);
    }
}
