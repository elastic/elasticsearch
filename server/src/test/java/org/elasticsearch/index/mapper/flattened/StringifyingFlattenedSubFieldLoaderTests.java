/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that {@link StringifyingFlattenedSubFieldLoader} renders a mapped sub-field's value as a string
 * when it is merged into the flattened root, so the root is the same stringly-typed blob on every loading path.
 */
public class StringifyingFlattenedSubFieldLoaderTests extends ESTestCase {

    public void testLongIsRenderedAsString() throws IOException {
        assertStringified(b -> b.field("status_code", 200L), "{\"status_code\":\"200\"}");
    }

    public void testDoubleIsRenderedAsString() throws IOException {
        assertStringified(b -> b.field("score", 1.5), "{\"score\":\"1.5\"}");
    }

    public void testBooleanIsRenderedAsString() throws IOException {
        assertStringified(b -> b.field("ok", true), "{\"ok\":\"true\"}");
    }

    public void testAlreadyStringValueIsUnchanged() throws IOException {
        assertStringified(b -> b.field("host.ip", "10.0.0.1"), "{\"host.ip\":\"10.0.0.1\"}");
    }

    public void testArrayOfScalarsIsStringified() throws IOException {
        assertStringified(b -> {
            b.startArray("codes");
            b.value(200L);
            b.value(404L);
            b.endArray();
        }, "{\"codes\":[\"200\",\"404\"]}");
    }

    public void testNullIsPreserved() throws IOException {
        assertStringified(b -> {
            b.field("missing");
            b.nullValue();
        }, "{\"missing\":null}");
    }

    private static void assertStringified(CheckedConsumer<XContentBuilder, IOException> nativeWrite, String expectedJson)
        throws IOException {
        StringifyingFlattenedSubFieldLoader loader = new StringifyingFlattenedSubFieldLoader(new WriteOnlyLoader(nativeWrite));
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            loader.write(b);
            b.endObject();
            assertThat(Strings.toString(b), equalTo(expectedJson));
        }
    }

    /**
     * A minimal stand-in for a typed sub-field's synthetic loader. A real loader reads doc values to decide
     * what to write; here only the {@link #write} behavior matters for the wrapper under test, so the rest is
     * stubbed to the simplest values that let {@code write} run.
     */
    private static final class WriteOnlyLoader implements SourceLoader.SyntheticFieldLoader {
        private final CheckedConsumer<XContentBuilder, IOException> nativeWrite;

        WriteOnlyLoader(CheckedConsumer<XContentBuilder, IOException> nativeWrite) {
            this.nativeWrite = nativeWrite;
        }

        @Override
        public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
            return Stream.empty();
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) {
            return null;
        }

        @Override
        public boolean hasValue() {
            return true;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            nativeWrite.accept(b);
        }

        @Override
        public void reset() {}

        @Override
        public String fieldName() {
            return "test";
        }
    }
}
