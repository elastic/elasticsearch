/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class ColumnarExistsNullSemanticsTests extends MapperServiceTestCase {

    private static final String FIELD = "field";
    private static final String SEQUENCE = "sequence";

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
        assertExistsMatchesOnlyDocumentsWithANonNullValue("date", ColumnarExistsNullSemanticsTests::randomDate);
    }

    public void testIpExistsMatchesOnlyDocumentsWithANonNullValue() throws IOException {
        assertExistsMatchesOnlyDocumentsWithANonNullValue("ip", () -> NetworkAddress.format(randomIp(randomBoolean())));
    }

    public void testBooleanExistsMatchesOnlyDocumentsWithANonNullValue() throws IOException {
        assertExistsMatchesOnlyDocumentsWithANonNullValue("boolean", () -> randomBoolean());
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/ISSUE_TO_BE_FILED")
    public void testKeywordSyntheticSourceDropsNullElements() throws IOException {
        assertSyntheticSourceDropsNullElements("keyword", () -> randomAlphaOfLengthBetween(1, 8));
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/ISSUE_TO_BE_FILED")
    public void testTextSyntheticSourceDropsNullElements() throws IOException {
        assertSyntheticSourceDropsNullElements("text", () -> randomAlphaOfLengthBetween(1, 8));
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/ISSUE_TO_BE_FILED")
    public void testLongSyntheticSourceDropsNullElements() throws IOException {
        assertSyntheticSourceDropsNullElements("long", () -> randomLong());
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/ISSUE_TO_BE_FILED")
    public void testDateSyntheticSourceDropsNullElements() throws IOException {
        assertSyntheticSourceDropsNullElements("date", ColumnarExistsNullSemanticsTests::randomDate);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/ISSUE_TO_BE_FILED")
    public void testIpSyntheticSourceDropsNullElements() throws IOException {
        assertSyntheticSourceDropsNullElements("ip", () -> NetworkAddress.format(randomIp(randomBoolean())));
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/ISSUE_TO_BE_FILED")
    public void testBooleanSyntheticSourceDropsNullElements() throws IOException {
        assertSyntheticSourceDropsNullElements("boolean", () -> randomBoolean());
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/165521")
    public void testKeywordExistsCountAgreesWithHits() throws IOException {
        assertExistsCountAgreesWithHits("keyword", () -> randomAlphaOfLengthBetween(1, 8));
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/165521")
    public void testTextExistsCountAgreesWithHits() throws IOException {
        assertExistsCountAgreesWithHits("text", () -> randomAlphaOfLengthBetween(1, 8));
    }

    public void testLongExistsCountAgreesWithHits() throws IOException {
        assertExistsCountAgreesWithHits("long", () -> randomLong());
    }

    private void assertExistsCountAgreesWithHits(String type, Supplier<Object> valueSupplier) throws IOException {
        final List<TestDocument> documents = randomDocuments(valueSupplier);
        documents.add(new TestDocument(Shape.ALL_NULL_ARRAY, new ArrayList<>(Collections.singletonList(null))));
        documents.add(new TestDocument(Shape.SCALAR, new ArrayList<>(List.of(valueSupplier.get()))));
        for (final boolean withCodec : new boolean[] { true, false }) {
            final MapperService mapperService = createColumnarMapperService(type, true, withCodec);
            withLuceneIndex(mapperService, iw -> {
                for (final TestDocument document : documents) {
                    iw.addDocument(mapperService.documentMapper().parse(source(b -> write(b, document))).rootDoc());
                }
            }, reader -> {
                final Query exists = mapperService.fieldType(FIELD).existsQuery(createSearchExecutionContext(mapperService));
                final IndexSearcher searcher = new IndexSearcher(reader);
                assertEquals(
                    "codec=" + withCodec + " documents=" + documents,
                    searcher.search(exists, documents.size()).scoreDocs.length,
                    searcher.count(exists)
                );
            });
        }
    }

    private void assertSyntheticSourceDropsNullElements(String type, Supplier<Object> valueSupplier) throws IOException {
        final List<TestDocument> documents = randomDocuments(valueSupplier);
        final boolean indexed = randomBoolean();
        for (final boolean withCodec : new boolean[] { true, false }) {
            final MapperService mapperService = createColumnarMapperService(type, indexed, withCodec);
            for (final TestDocument document : documents) {
                assertEquals(
                    "codec=" + withCodec + " index=" + indexed + " document=" + document,
                    expectedSource(document),
                    syntheticSource(mapperService.documentMapper(), b -> write(b, document))
                );
            }
        }
    }

    private void assertExistsMatchesOnlyDocumentsWithANonNullValue(String type, Supplier<Object> valueSupplier) throws IOException {
        final List<TestDocument> documents = randomDocuments(valueSupplier);
        final List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < documents.size(); i++) {
            if (documents.get(i).hasValue()) {
                expected.add(i);
            }
        }
        final boolean indexed = randomBoolean();
        for (final boolean withCodec : new boolean[] { true, false }) {
            final MapperService mapperService = createColumnarMapperService(type, indexed, withCodec);
            final String message = "codec=" + withCodec + " index=" + indexed + " documents=" + documents;
            withLuceneIndex(mapperService, iw -> {
                for (int i = 0; i < documents.size(); i++) {
                    final TestDocument document = documents.get(i);
                    final LuceneDocument luceneDocument = mapperService.documentMapper().parse(source(b -> write(b, document))).rootDoc();
                    luceneDocument.add(new StoredField(SEQUENCE, i));
                    iw.addDocument(luceneDocument);
                    if (rarely()) {
                        iw.commit();
                    }
                }
            }, reader -> {
                final Query exists = mapperService.fieldType(FIELD).existsQuery(createSearchExecutionContext(mapperService));
                final IndexSearcher searcher = new IndexSearcher(reader);
                final List<Integer> matched = new ArrayList<>();
                for (final ScoreDoc hit : searcher.search(exists, documents.size()).scoreDocs) {
                    matched.add(searcher.storedFields().document(hit.doc).getField(SEQUENCE).numericValue().intValue());
                }
                Collections.sort(matched);
                assertEquals(message, expected, matched);
                assertEquals(message, expected.size(), searcher.count(exists));
            });
        }
    }

    private MapperService createColumnarMapperService(String type, boolean indexed, boolean withCodec) throws IOException {
        return createMapperService(
            withCodec ? IndexVersion.current() : IndexVersionUtils.getPreviousVersion(IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG),
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(),
            mapping(b -> b.startObject(FIELD).field("type", type).field("index", indexed).endObject())
        );
    }

    private static String randomDate() {
        return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(randomLongBetween(0, 4_102_444_800_000L));
    }

    private static List<TestDocument> randomDocuments(Supplier<Object> valueSupplier) {
        final List<TestDocument> documents = new ArrayList<>();
        for (int i = between(8, 40); i > 0; i--) {
            documents.add(randomDocument(valueSupplier));
        }
        return documents;
    }

    private static String expectedSource(TestDocument document) throws IOException {
        final XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        final List<Object> values = document.slots().stream().filter(Objects::nonNull).toList();
        if (values.size() == 1) {
            builder.field(FIELD, values.get(0));
        } else if (values.isEmpty() == false) {
            builder.field(FIELD, values);
        }
        return Strings.toString(builder.endObject());
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
