/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class ColumnarExistsNullSemanticsTests extends MapperServiceTestCase {

    private static final String FIELD = "field";

    private enum Shape {
        ABSENT,
        BARE_NULL,
        SCALAR,
        ARRAY,
        ALL_NULL_ARRAY
    }

    private record TestDocument(Shape shape, List<Object> slots) {
        boolean hasValue() {
            return slots.stream().anyMatch(Objects::nonNull);
        }
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/165521")
    public void testKeywordExistsMatchesOnlyDocumentsWithANonNullValue() throws IOException {
        assertExistsMatchesOnlyDocumentsWithANonNullValue("keyword", () -> randomAlphaOfLengthBetween(1, 8));
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/165521")
    public void testTextExistsMatchesOnlyDocumentsWithANonNullValue() throws IOException {
        assertExistsMatchesOnlyDocumentsWithANonNullValue("text", () -> randomAlphaOfLengthBetween(1, 8));
    }

    public void testLongExistsMatchesOnlyDocumentsWithANonNullValue() throws IOException {
        assertExistsMatchesOnlyDocumentsWithANonNullValue("long", () -> randomLong());
    }

    public void testDateExistsMatchesOnlyDocumentsWithANonNullValue() throws IOException {
        assertExistsMatchesOnlyDocumentsWithANonNullValue(
            "date",
            () -> Instant.ofEpochMilli(randomLongBetween(0, 4_102_444_800_000L)).toString()
        );
    }

    private void assertExistsMatchesOnlyDocumentsWithANonNullValue(String type, Supplier<Object> valueSupplier) throws IOException {
        final List<TestDocument> documents = new ArrayList<>();
        for (int i = between(8, 40); i > 0; i--) {
            documents.add(randomDocument(valueSupplier));
        }
        final List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < documents.size(); i++) {
            if (documents.get(i).hasValue()) {
                expected.add(i);
            }
        }
        final boolean indexed = randomBoolean();
        for (final boolean withCodec : new boolean[] { true, false }) {
            final MapperService mapperService = createMapperService(
                withCodec ? IndexVersion.current() : IndexVersionUtils.getPreviousVersion(IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG),
                Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(),
                mapping(b -> b.startObject(FIELD).field("type", type).field("index", indexed).endObject())
            );
            final String message = "codec=" + withCodec + " index=" + indexed + " documents=" + documents;
            withLuceneIndex(mapperService, iw -> {
                for (final TestDocument document : documents) {
                    iw.addDocument(mapperService.documentMapper().parse(source(b -> write(b, document))).rootDoc());
                    if (rarely()) {
                        iw.commit();
                    }
                }
            }, reader -> {
                final Query exists = mapperService.fieldType(FIELD).existsQuery(createSearchExecutionContext(mapperService));
                final IndexSearcher searcher = new IndexSearcher(reader);
                final List<Integer> matched = new ArrayList<>();
                for (final ScoreDoc hit : searcher.search(exists, documents.size()).scoreDocs) {
                    matched.add(hit.doc);
                }
                Collections.sort(matched);
                assertEquals(message, expected, matched);
                assertEquals(message, expected.size(), searcher.count(exists));
            });
        }
    }

    private static TestDocument randomDocument(Supplier<Object> valueSupplier) {
        final Shape shape = randomFrom(Shape.values());
        final List<Object> slots = new ArrayList<>();
        switch (shape) {
            case ABSENT, BARE_NULL -> {
            }
            case SCALAR -> slots.add(valueSupplier.get());
            case ARRAY -> {
                for (int i = between(0, 4); i > 0; i--) {
                    slots.add(randomBoolean() ? null : valueSupplier.get());
                }
            }
            case ALL_NULL_ARRAY -> {
                for (int i = between(1, 3); i > 0; i--) {
                    slots.add(null);
                }
            }
        }
        return new TestDocument(shape, slots);
    }

    private static void write(XContentBuilder builder, TestDocument document) throws IOException {
        switch (document.shape()) {
            case ABSENT -> {
            }
            case BARE_NULL -> builder.nullField(FIELD);
            case SCALAR -> builder.field(FIELD, document.slots().get(0));
            case ARRAY, ALL_NULL_ARRAY -> {
                builder.startArray(FIELD);
                for (final Object slot : document.slots()) {
                    if (slot == null) {
                        builder.nullValue();
                    } else {
                        builder.value(slot);
                    }
                }
                builder.endArray();
            }
        }
    }
}
