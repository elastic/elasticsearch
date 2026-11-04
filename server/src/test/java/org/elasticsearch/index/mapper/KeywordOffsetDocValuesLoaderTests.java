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
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
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
     * When an array's encoded offsets exceed {@link IndexWriter#MAX_TERM_LENGTH} and some of its values were ignored via
     * {@code ignore_above}, the array is still moved to {@code _ignored_source}. The entry suppresses the field's own storage of
     * ignored values at load time, so it carries the ignored values itself, at their original position in the array.
     */
    public void testOversizedOffsetArrayWithIgnoredValuesFallsBackToIgnoredSource() throws Exception {
        int numSlots = IndexWriter.MAX_TERM_LENGTH + between(1, 500);
        List<Object> values = new ArrayList<>(numSlots);
        for (int i = 0; i < numSlots; i++) {
            values.add(randomBoolean() && randomBoolean() ? null : randomValue());
        }
        // Values longer than ignore_above get no offsets slot and are stored in the field's own fallback storage. Cover the
        // first and last position, several in between, and two adjacent ones.
        values.addFirst(randomAlphaOfLength(20));
        values.addLast(randomAlphaOfLength(20));
        int adjacent = between(1, values.size() - 1);
        values.add(adjacent, randomAlphaOfLength(20));
        values.add(adjacent, randomAlphaOfLength(20));
        for (int i = 0; i < between(1, 10); i++) {
            values.add(between(1, values.size() - 1), randomAlphaOfLength(20));
        }

        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties").startObject("field");
        minimalMapping(mapping);
        mapping.field("ignore_above", 10);
        mapping.endObject().endObject().endObject().endObject();
        try (var mapperService = createMapperService(mapping)) {
            var mapper = mapperService.documentMapper();

            var doc = mapper.parse(source(b -> b.field("field", values)));
            assertThat(doc.rootDoc().getFields("field.offsets"), Matchers.empty());

            String expected;
            try (XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent())) {
                b.startObject().field("field", values).endObject();
                expected = Strings.toString(b);
            }
            assertThat(syntheticSource(mapper, b -> b.field("field", values)), Matchers.equalTo(expected));
        }
    }

    /**
     * Like {@link #testAllIgnoredArrayWritesNoOffsets()}, with values ignored via {@code ignore_above}: no offsets doc value is
     * written for an array whose values were all ignored, and synthetic source comes from the field's own storage of ignored values.
     */
    public void testAllIgnoredViaIgnoreAboveArrayWritesNoOffsets() throws Exception {
        int numValues = randomBoolean() ? between(1, 10) : IndexWriter.MAX_TERM_LENGTH + between(1, 500);
        List<Object> values = new ArrayList<>(numValues);
        for (int i = 0; i < numValues; i++) {
            values.add(randomAlphaOfLength(20));
        }

        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties").startObject("field");
        minimalMapping(mapping);
        mapping.field("ignore_above", 10);
        mapping.endObject().endObject().endObject().endObject();
        try (var mapperService = createMapperService(mapping)) {
            var mapper = mapperService.documentMapper();

            var doc = mapper.parse(source(b -> b.field("field", values)));
            assertThat(doc.rootDoc().getFields("field.offsets"), Matchers.empty());
            assertSyntheticSourceHasIgnoredValues(syntheticSource(mapper, b -> b.field("field", values)), values);
        }
    }

    @Override
    protected Object randomMalformedValue() {
        // keyword has no ignore_malformed; see testOversizedOffsetArrayWithIgnoredValuesFallsBackToIgnoredSource for ignore_above
        return null;
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
