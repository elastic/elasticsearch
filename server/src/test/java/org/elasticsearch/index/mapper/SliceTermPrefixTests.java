/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.suggest.document.SuggestField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests that a slice index writes the slice as a prefix of every indexed term while doc values, points and stored values keep
 * the plain value. The first half drives {@link SliceTermPrefix} through {@link LuceneDocument} with hand-built Lucene fields,
 * one per shape it has to recognize; the second half goes through the mappers and the resulting Lucene index.
 */
public class SliceTermPrefixTests extends MapperServiceTestCase {

    private static final String SLICE = "tenant-1";
    private static final String PREFIX = SliceIndexing.termPrefix(SLICE);

    private static String prefix(String slice) {
        return SliceIndexing.termPrefix(slice);
    }

    @Before
    public void requireSliceIndexing() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
    }

    private static List<IndexableField> rewrite(IndexableField... fields) {
        LuceneDocument document = new LuceneDocument(new SliceTermPrefix(SLICE));
        for (IndexableField field : fields) {
            document.add(field);
        }
        return document.getFields();
    }

    private static List<String> tokensOf(IndexableField field) throws IOException {
        final List<String> tokens = new ArrayList<>();
        try (TokenStream stream = field.tokenStream(Lucene.STANDARD_ANALYZER, null)) {
            final CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                tokens.add(term.toString());
            }
            stream.end();
        }
        return tokens;
    }

    private static FieldType indexedType(boolean docValues, boolean stored) {
        FieldType type = new FieldType();
        type.setIndexOptions(IndexOptions.DOCS);
        type.setTokenized(false);
        type.setOmitNorms(true);
        type.setStored(stored);
        if (docValues) {
            type.setDocValuesType(DocValuesType.SORTED_SET);
        }
        type.freeze();
        return type;
    }

    public void testAnIndexOnlyTermIsPrefixedWithoutSplittingTheField() {
        List<IndexableField> fields = rewrite(
            new KeywordFieldMapper.KeywordField("field", new BytesRef("value"), indexedType(false, false))
        );
        assertThat(fields, hasSize(1));
        assertThat(fields.get(0).name(), equalTo("field"));
        assertThat(fields.get(0).binaryValue(), equalTo(new BytesRef(PREFIX + "value")));
        assertThat(fields.get(0).fieldType().indexOptions(), equalTo(IndexOptions.DOCS));
    }

    public void testADocValuesFieldIsSplitSoOnlyItsTermCarriesThePrefix() {
        List<IndexableField> fields = rewrite(
            new KeywordFieldMapper.KeywordField("field", new BytesRef("value"), indexedType(true, false))
        );
        assertThat(fields, hasSize(2));

        IndexableField indexed = fields.get(0);
        assertThat(indexed.binaryValue(), equalTo(new BytesRef(PREFIX + "value")));
        assertThat(indexed.fieldType().indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(indexed.fieldType().docValuesType(), equalTo(DocValuesType.NONE));

        IndexableField plain = fields.get(1);
        assertThat(plain.binaryValue(), equalTo(new BytesRef("value")));
        assertThat(plain.fieldType().indexOptions(), equalTo(IndexOptions.NONE));
        assertThat(plain.fieldType().docValuesType(), equalTo(DocValuesType.SORTED_SET));
    }

    public void testAStoredFieldIsSplitSoTheStoredValueStaysPlain() {
        List<IndexableField> fields = rewrite(
            new KeywordFieldMapper.KeywordField("field", new BytesRef("value"), indexedType(false, true))
        );
        assertThat(fields, hasSize(2));
        assertFalse(fields.get(0).fieldType().stored());
        assertThat(fields.get(0).binaryValue(), equalTo(new BytesRef(PREFIX + "value")));
        assertTrue(fields.get(1).fieldType().stored());
        assertThat(fields.get(1).binaryValue(), equalTo(new BytesRef("value")));
        assertThat(fields.get(1).fieldType().indexOptions(), equalTo(IndexOptions.NONE));
    }

    public void testAPointFieldIsSplitSoThePointKeepsThePlainValue() {
        final FieldType type = new FieldType();
        type.setIndexOptions(IndexOptions.DOCS);
        type.setTokenized(false);
        type.setDimensions(1, 1, 5);
        type.freeze();
        List<IndexableField> fields = rewrite(new Field("field", new BytesRef("value"), type) {
            @Override
            public InvertableType invertableType() {
                return InvertableType.BINARY;
            }
        });
        assertThat(fields, hasSize(2));

        IndexableField indexed = fields.get(0);
        assertThat(indexed.binaryValue(), equalTo(new BytesRef(PREFIX + "value")));
        assertThat(indexed.fieldType().pointDimensionCount(), equalTo(0));

        IndexableField plain = fields.get(1);
        assertThat(plain.binaryValue(), equalTo(new BytesRef("value")));
        assertThat(plain.fieldType().indexOptions(), equalTo(IndexOptions.NONE));
        assertThat(plain.fieldType().pointDimensionCount(), equalTo(1));
        assertThat(plain.fieldType().pointNumBytes(), equalTo(5));
    }

    public void testEveryTokenOfATokenStreamIsPrefixed() throws IOException {
        FieldType type = new FieldType();
        type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        type.setTokenized(true);
        type.freeze();

        List<IndexableField> fields = rewrite(new Field("field", "the quick brown fox", type));
        assertThat(fields, hasSize(1));

        List<String> tokens = new ArrayList<>();
        List<Integer> increments = new ArrayList<>();
        try (TokenStream stream = fields.get(0).tokenStream(Lucene.STANDARD_ANALYZER, null)) {
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute increment = stream.addAttribute(PositionIncrementAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                tokens.add(term.toString());
                increments.add(increment.getPositionIncrement());
            }
            stream.end();
        }
        assertThat(tokens, contains(PREFIX + "the", PREFIX + "quick", PREFIX + "brown", PREFIX + "fox"));
        assertThat(increments, contains(1, 1, 1, 1));
    }

    public void testAFieldThatIndexesNoTermsIsLeftAlone() {
        IndexableField docValues = new SortedSetDocValuesField("field", new BytesRef("value"));
        IndexableField point = new LongPoint("field", 42);
        List<IndexableField> fields = rewrite(docValues, point);
        assertThat(fields.get(0), sameInstance(docValues));
        assertThat(fields.get(1), sameInstance(point));
    }

    /**
     * A {@link FeatureField}, which is how {@code sparse_vector} and {@code rank_features} index a feature, is prefixed like any
     * other term: its queries are built through the field type, which puts the same prefix on. See
     * {@code SliceFeatureFieldTests} for the weight it keeps in the term frequency.
     */
    public void testAFeatureFieldIsPrefixed() throws IOException {
        IndexableField feature = new FeatureField("sparse", "mytoken", 3.0f);
        IndexableField rewritten = rewrite(feature).get(0);
        assertThat(rewritten, not(sameInstance(feature)));
        assertThat(tokensOf(rewritten), contains(PREFIX + "mytoken"));
    }

    /** {@code completion} encodes a surface form for a transducer and is suggested over, never asked for by value. */
    public void testACompletionFieldKeepsPlainTerms() {
        IndexableField suggest = new SuggestField("suggest", "surfaceform", 5);
        assertThat(rewrite(suggest).get(0), sameInstance(suggest));
    }

    public void testReservedFieldNamesKeepPlainTerms() {
        IndexableField id = new StringField("_id", "the-id", Field.Store.YES);
        assertThat(rewrite(id).get(0), sameInstance(id));
        assertFalse(SliceIndexing.prefixesTerms("_routing"));
        assertFalse(SliceIndexing.prefixesTerms(SliceIndexing.FIELD_NAME));
        assertFalse(SliceIndexing.prefixesTerms(FieldNamesFieldMapper.NAME));
        assertTrue(SliceIndexing.prefixesTerms("field"));
    }

    public void testTheRewrittenFieldReadsThroughToTheOriginal() {
        // A mapper can keep hold of the field it added and set its value afterwards; the prefix is applied when Lucene reads it.
        Field original = new Field("field", new BytesRef("first"), indexedType(false, false)) {
            @Override
            public InvertableType invertableType() {
                return InvertableType.BINARY;
            }
        };
        IndexableField rewritten = rewrite(original).get(0);
        assertThat(rewritten.binaryValue(), equalTo(new BytesRef(PREFIX + "first")));
        original.setBytesValue(new BytesRef("second"));
        assertThat(rewritten.binaryValue(), equalTo(new BytesRef(PREFIX + "second")));
    }

    private MapperService sliceMapperService(CheckedConsumer<XContentBuilder, IOException> fields) throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build();
        return createMapperService(settings, mapping(fields));
    }

    public void testAKeywordIsIndexedUnderTheSliceAndStaysPlainInDocValues() throws IOException {
        MapperService mapperService = sliceMapperService(b -> b.startObject("field").field("type", "keyword").endObject());
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(mapperService.documentMapper().parse(source("1", b -> b.field("field", "value"), SLICE)).rootDoc());
        }, reader -> {
            assertThat(terms(reader, "field"), contains(PREFIX + "value"));
            assertThat(docValues(reader, "field"), contains("value"));
        });
    }

    public void testTheTermsOfTwoSlicesSortIntoContiguousRuns() throws IOException {
        MapperService mapperService = sliceMapperService(b -> b.startObject("field").field("type", "keyword").endObject());
        withLuceneIndex(mapperService, writer -> {
            for (String value : List.of("alpha", "beta")) {
                for (String slice : List.of("tenant-1", "tenant-2")) {
                    writer.addDocument(
                        mapperService.documentMapper().parse(source(slice + value, b -> b.field("field", value), slice)).rootDoc()
                    );
                }
            }
        }, reader -> {
            final List<String> expected = new ArrayList<>();
            for (String slice : List.of("tenant-1", "tenant-2")) {
                expected.add(prefix(slice) + "alpha");
                expected.add(prefix(slice) + "beta");
            }
            // slices group by hash prefix, so the runs are adjacent whatever order the hashes fall in
            expected.sort(String::compareTo);
            assertThat(terms(reader, "field"), equalTo(expected));
        });
    }

    public void testATextFieldIsIndexedTokenByTokenUnderTheSlice() throws IOException {
        MapperService mapperService = sliceMapperService(b -> b.startObject("field").field("type", "text").endObject());
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(
                mapperService.documentMapper().parse(source("1", b -> b.field("field", "quick brown fox"), SLICE)).rootDoc()
            );
        }, reader -> assertThat(terms(reader, "field"), contains(PREFIX + "brown", PREFIX + "fox", PREFIX + "quick")));
    }

    public void testConsecutiveTextDocumentsDoNotInheritEachOthersSlice() throws IOException {
        // Lucene reuses one token stream per field across the documents of a buffer, so the prefix of the previous
        // document must not survive into the next one.
        MapperService mapperService = sliceMapperService(b -> b.startObject("field").field("type", "text").endObject());
        withLuceneIndex(mapperService, writer -> {
            for (String slice : List.of("tenant-1", "tenant-2", "tenant-3")) {
                final String word = switch (slice) {
                    case "tenant-1" -> "one";
                    case "tenant-2" -> "two";
                    default -> "three";
                };
                writer.addDocument(mapperService.documentMapper().parse(source(slice, b -> b.field("field", word), slice)).rootDoc());
            }
        }, reader -> {
            final List<String> expected = new ArrayList<>(
                List.of(prefix("tenant-1") + "one", prefix("tenant-2") + "two", prefix("tenant-3") + "three")
            );
            expected.sort(String::compareTo);
            assertThat(terms(reader, "field"), equalTo(expected));
        });
    }

    public void testMultiFieldsAndCopyToAreIndexedUnderTheSlice() throws IOException {
        MapperService mapperService = sliceMapperService(b -> {
            b.startObject("field");
            {
                b.field("type", "text");
                b.field("copy_to", "copy");
                b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            }
            b.endObject();
            b.startObject("copy").field("type", "keyword").endObject();
        });
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(mapperService.documentMapper().parse(source("1", b -> b.field("field", "value"), SLICE)).rootDoc());
        }, reader -> {
            assertThat(terms(reader, "field"), contains(PREFIX + "value"));
            assertThat(terms(reader, "field.raw"), contains(PREFIX + "value"));
            assertThat(terms(reader, "copy"), contains(PREFIX + "value"));
        });
    }

    public void testNestedDocumentsAreIndexedUnderTheSliceOfTheirRoot() throws IOException {
        MapperService mapperService = sliceMapperService(b -> {
            b.startObject("outer");
            {
                b.field("type", "nested");
                b.startObject("properties").startObject("inner").field("type", "keyword").endObject().endObject();
            }
            b.endObject();
        });
        ParsedDocument parsed = mapperService.documentMapper()
            .parse(source("1", b -> b.startArray("outer").startObject().field("inner", "value").endObject().endArray(), SLICE));
        assertThat(parsed.docs(), hasSize(2));
        LuceneDocument nested = parsed.docs().get(0);
        assertThat(indexedTerm(nested, "outer.inner"), equalTo(new BytesRef(PREFIX + "value")));
        assertThat(indexedTerm(nested, NestedPathFieldMapper.NAME), equalTo(new BytesRef("outer")));
    }

    public void testAStoredKeywordReturnsThePlainValue() throws IOException {
        MapperService mapperService = sliceMapperService(
            b -> b.startObject("field").field("type", "keyword").field("store", true).endObject()
        );
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(mapperService.documentMapper().parse(source("1", b -> b.field("field", "value"), SLICE)).rootDoc());
        }, reader -> {
            assertThat(terms(reader, "field"), contains(PREFIX + "value"));
            assertThat(reader.storedFields().document(0).getBinaryValue("field"), equalTo(new BytesRef("value")));
        });
    }

    public void testEveryMappedTermMatchesAnIndexWithoutSlicesPrefixForPrefix() throws IOException {
        CheckedConsumer<XContentBuilder, IOException> mappings = b -> {
            b.startObject("keyword").field("type", "keyword").endObject();
            b.startObject("text").field("type", "text").endObject();
            b.startObject("count").field("type", "long").endObject();
            b.startObject("address").field("type", "ip").endObject();
            b.startObject("flag").field("type", "boolean").endObject();
            b.startObject("when").field("type", "date").endObject();
        };
        CheckedConsumer<XContentBuilder, IOException> document = b -> {
            b.field("keyword", "blue");
            b.field("text", "quick brown fox");
            b.field("count", 42);
            b.field("address", "192.168.0.1");
            b.field("flag", true);
            b.field("when", "2026-10-05T00:00:00Z");
        };

        // _routing and _field_names are indexed differently by a slice index, so they are checked on their own below.
        List<String> fields = List.of("keyword", "text", "count", "address", "flag", "when");
        Map<String, List<String>> sliced = termsByField(sliceMapperService(mappings), document, fields);
        Map<String, List<String>> plain = termsByField(createMapperService(mapping(mappings)), document, fields);

        for (String field : fields) {
            List<String> expected = plain.getOrDefault(field, List.of())
                .stream()
                .map(term -> SliceIndexing.prefixesTerms(field) ? PREFIX + term : term)
                .toList();
            assertThat(field, sliced.getOrDefault(field, List.of()), equalTo(expected));
        }
    }

    public void testReservedFieldsAreIndexedWithoutThePrefix() throws IOException {
        MapperService mapperService = sliceMapperService(b -> b.startObject("field").field("type", "keyword").endObject());
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(mapperService.documentMapper().parse(source("1", b -> b.field("field", "value"), SLICE)).rootDoc());
        }, reader -> {
            for (LeafReaderContext leaf : reader.leaves()) {
                for (var info : leaf.reader().getFieldInfos()) {
                    if (info.getIndexOptions() == IndexOptions.NONE || SliceIndexing.prefixesTerms(info.name)) {
                        continue;
                    }
                    Terms terms = leaf.reader().terms(info.name);
                    TermsEnum iterator = terms.iterator();
                    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
                        assertThat(info.name, StringHelper.startsWith(term, new BytesRef(PREFIX)), equalTo(false));
                    }
                }
            }
        });
    }

    public void testAnIndexWithoutSlicesIsUnchanged() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("field").field("type", "keyword").endObject()));
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(mapperService.documentMapper().parse(source("1", b -> b.field("field", "value"), SLICE)).rootDoc());
        }, reader -> {
            assertThat(terms(reader, "field"), contains("value"));
            assertThat(docValues(reader, "field"), contains("value"));
        });
    }

    private Map<String, List<String>> termsByField(
        MapperService mapperService,
        CheckedConsumer<XContentBuilder, IOException> document,
        List<String> fields
    ) throws IOException {
        Map<String, List<String>> byField = new TreeMap<>();
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(mapperService.documentMapper().parse(source("1", document, SLICE)).rootDoc());
        }, reader -> {
            for (String field : fields) {
                byField.put(field, terms(reader, field));
            }
        });
        return byField;
    }

    /** The one value of {@code field} that goes into the terms dictionary. */
    private static BytesRef indexedTerm(LuceneDocument document, String field) {
        List<BytesRef> terms = new ArrayList<>();
        for (IndexableField candidate : document.getFields()) {
            if (candidate.name().equals(field) && candidate.fieldType().indexOptions() != IndexOptions.NONE) {
                terms.add(candidate.binaryValue());
            }
        }
        assertThat(field, terms, hasSize(1));
        return terms.get(0);
    }

    private static List<String> terms(DirectoryReader reader, String field) throws IOException {
        List<String> values = new ArrayList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            Terms terms = leaf.reader().terms(field);
            if (terms == null) {
                continue;
            }
            TermsEnum iterator = terms.iterator();
            for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
                values.add(term.utf8ToString());
            }
        }
        return values;
    }

    private static List<String> docValues(DirectoryReader reader, String field) throws IOException {
        List<String> values = new ArrayList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            SortedSetDocValues docValues = leaf.reader().getSortedSetDocValues(field);
            if (docValues == null) {
                continue;
            }
            for (int doc = docValues.nextDoc(); doc != NO_MORE_DOCS; doc = docValues.nextDoc()) {
                for (int i = 0; i < docValues.docValueCount(); i++) {
                    values.add(docValues.lookupOrd(docValues.nextOrd()).utf8ToString());
                }
            }
        }
        return values;
    }
}
