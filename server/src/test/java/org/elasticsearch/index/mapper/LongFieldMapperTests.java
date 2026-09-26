/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class LongFieldMapperTests extends WholeNumberFieldMapperTests {

    @Override
    protected Number missingValue() {
        return 123L;
    }

    @Override
    protected List<NumberTypeOutOfRangeSpec> outOfRangeSpecs() {
        return List.of(
            NumberTypeOutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, "9223372036854775808", "out of range for a long"),
            NumberTypeOutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, "1e999999999", "out of range for a long"),
            NumberTypeOutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, "-9223372036854775809", "out of range for a long"),
            NumberTypeOutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, "-1e999999999", "out of range for a long"),
            NumberTypeOutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, new BigInteger("9223372036854775808"), "out of range of long"),
            NumberTypeOutOfRangeSpec.of(NumberFieldMapper.NumberType.LONG, new BigInteger("-9223372036854775809"), "out of range of long")
        );
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        super.registerParameters(checker);
        checker.registerConflictCheck("index_terms", b -> b.field("index_terms", true));
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "long");
    }

    @Override
    protected boolean allowsIndexTimeScript() {
        return true;
    }

    public void testScriptAndPrecludedParameters() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "long");
                b.field("script", "test");
                b.field("coerce", "true");
            })));
            assertThat(e.getMessage(), equalTo("Failed to parse mapping: Field [coerce] cannot be set in conjunction with field [script]"));
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "long");
                b.field("script", "test");
                b.field("null_value", 7);
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [null_value] cannot be set in conjunction with field [script]")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "long");
                b.field("script", "test");
                b.field("ignore_malformed", "true");
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [ignore_malformed] cannot be set in conjunction with field [script]")
            );
        }
    }

    public void testLongIndexingOutOfRange() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long").field("ignore_malformed", true)));
        ParsedDocument doc = mapper.parse(
            source(b -> b.rawField("field", new BytesArray("9223372036854775808").streamInput(), XContentType.JSON))
        );
        assertThat(doc.rootDoc().getFields("field"), empty());
    }

    public void testLongIndexingCoercesIntoRange() throws Exception {
        // the following two strings are in-range for a long after coercion
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "9223372036854775807.9")));
        assertThat(doc.rootDoc().getFields("field"), hasSize(1));
        doc = mapper.parse(source(b -> b.field("field", "-9223372036854775808.9")));
        assertThat(doc.rootDoc().getFields("field"), hasSize(1));
    }

    public void testLongIndexingRejectsOversizedString() throws Exception {
        // A quoted numeric value long enough to be costly to parse is rejected instead of coerced.
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        String oversized = "1." + "0".repeat(Numbers.MAX_NUMERIC_STRING_LENGTH);
        expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field("field", oversized))));
    }

    // This is the biggest long that double can represent exactly
    public static final long MAX_SAFE_LONG_FOR_DOUBLE = 1L << 53;

    @Override
    protected Number randomNumber() {
        if (randomBoolean()) {
            return randomLong();
        }
        if (randomBoolean()) {
            return randomDouble();
        }
        // TODO: increase the range back to full LONG range once https://github.com/elastic/elasticsearch/issues/132893 is fixed
        return randomDoubleBetween(-MAX_SAFE_LONG_FOR_DOUBLE, MAX_SAFE_LONG_FOR_DOUBLE, true);
    }

    protected IngestScriptSupport ingestScriptSupport() {
        return new IngestScriptSupport() {
            @Override
            @SuppressWarnings("unchecked")
            protected <T> T compileOtherScript(Script script, ScriptContext<T> context) {
                if (context == LongFieldScript.CONTEXT) {
                    return (T) LongFieldScript.PARSE_FROM_SOURCE;
                }
                throw new UnsupportedOperationException("Unknown script " + script.getIdOrCode());
            }

            @Override
            protected LongFieldScript.Factory emptyFieldScript() {
                return (fieldName, params, searchLookup, onScriptError) -> ctx -> new LongFieldScript(
                    fieldName,
                    params,
                    searchLookup,
                    OnScriptError.FAIL,
                    ctx
                ) {
                    @Override
                    public void execute() {}
                };
            }

            @Override
            protected LongFieldScript.Factory nonEmptyFieldScript() {
                return (fieldName, params, searchLookup, onScriptError) -> ctx -> new LongFieldScript(
                    fieldName,
                    params,
                    searchLookup,
                    OnScriptError.FAIL,
                    ctx
                ) {
                    @Override
                    public void execute() {
                        emit(1);
                    }
                };
            }
        };
    }

    protected boolean supportsBulkLongBlockReading() {
        return true;
    }

    /**
     * The sortable-bytes encoding covers the whole long range, so any value from Long.MIN_VALUE to
     * Long.MAX_VALUE is indexed and searchable -- negatives included. Both extremes are exercised
     * alongside a random value.
     */
    public void testIndexTermsIndexesSortableBytesTerms() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "long");
            b.field("index_terms", true);
        }));

        long value = randomFrom(Long.MIN_VALUE, Long.MAX_VALUE, randomLong());
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", value)));
        List<IndexableField> fields = doc.rootDoc().getFields("field");

        // Should have a terms field (inverted index) with the sortable-bytes encoded value
        long termsCount = fields.stream().filter(f -> f.fieldType().indexOptions().compareTo(IndexOptions.NONE) > 0).count();
        assertEquals(1, termsCount);
        IndexableField termsField = fields.stream()
            .filter(f -> f.fieldType().indexOptions().compareTo(IndexOptions.NONE) > 0)
            .findFirst()
            .get();
        byte[] expected = new byte[Long.BYTES];
        NumericUtils.longToSortableBytes(value, expected, 0);
        assertEquals(new BytesRef(expected), termsField.binaryValue());
        // Terms are the full 8 bytes: the integer-width encoding would still index and still look
        // sorted, but would never match a query term.
        assertEquals(Long.BYTES, termsField.binaryValue().length);

        // Should have doc values
        long dvCount = fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).count();
        assertEquals(1, dvCount);
        IndexableField dvField = fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).findFirst().get();
        assertEquals(value, dvField.numericValue().longValue());
    }

    public void testIndexTermsRequiresIndex() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "long");
            b.field("index_terms", true);
            b.field("index", false);
        })));
        assertThat(e.getMessage(), containsString("[index_terms] requires that [index] is true"));
    }

    public void testIndexTermsRejectedOnLegacyIndex() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(IndexVersion.fromId(5000099), fieldMapping(b -> {
            b.field("type", "long");
            b.field("index_terms", true);
        })));
        assertThat(e.getMessage(), containsString("[index_terms] is not supported on legacy indices"));
    }

    private static Set<Integer> matchingDocIds(IndexSearcher searcher, Query query) throws IOException {
        TopDocs topDocs = searcher.search(query, searcher.getIndexReader().maxDoc());
        Set<Integer> docIds = new TreeSet<>();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            docIds.add(scoreDoc.doc);
        }
        return docIds;
    }

    /**
     * Indexes the same random long values into a points-based field and two index_terms fields (one
     * with doc values, one without), across a random number of segments, and asserts that term,
     * terms and range queries match the exact same set of documents.
     */
    public void testIndexTermsMatchesPointsRandomized() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field1").field("type", "long").endObject();
            b.startObject("field2").field("type", "long").field("index_terms", true).endObject();
            b.startObject("field3").field("type", "long").field("index_terms", true).field("doc_values", false).endObject();
        }));

        int numDocs = randomIntBetween(500, 1000);
        long[] values = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            // Mix full-range longs with values near the int boundary, where a wrong-width or
            // double-routed encoding would be most likely to go unnoticed.
            values[i] = switch (randomIntBetween(0, 2)) {
                case 0 -> randomLong();
                case 1 -> randomLongBetween(Integer.MAX_VALUE - 10L, Integer.MAX_VALUE + 10L);
                default -> randomLongBetween(-100, 100);
            };
        }

        withLuceneIndex(mapperService, iw -> {
            for (long value : values) {
                ParsedDocument doc = mapperService.documentMapper()
                    .parse(source(b -> b.field("field1", value).field("field2", value).field("field3", value)));
                iw.addDocument(doc.rootDoc());
                // Occasionally commit so the index ends up with several segments rather than one.
                if (rarely()) {
                    iw.commit();
                }
            }
        }, ir -> {
            IndexSearcher searcher = newSearcher(ir);
            NumberFieldMapper.NumberFieldType ft1 = (NumberFieldMapper.NumberFieldType) mapperService.fieldType("field1");
            NumberFieldMapper.NumberFieldType ft2 = (NumberFieldMapper.NumberFieldType) mapperService.fieldType("field2");
            NumberFieldMapper.NumberFieldType ft3 = (NumberFieldMapper.NumberFieldType) mapperService.fieldType("field3");
            SearchExecutionContext context = createSearchExecutionContext(mapperService);

            int iters = 5;
            for (int iter = 0; iter < iters; iter++) {
                long termValue = randomBoolean() ? values[randomIntBetween(0, numDocs - 1)] : randomLong();
                Set<Integer> expectedTermDocs = matchingDocIds(searcher, ft1.termQuery(termValue, context));
                assertEquals(
                    "term query [" + termValue + "]",
                    expectedTermDocs,
                    matchingDocIds(searcher, ft2.termQuery(termValue, context))
                );
                assertEquals(
                    "term query [" + termValue + "]",
                    expectedTermDocs,
                    matchingDocIds(searcher, ft3.termQuery(termValue, context))
                );

                int numTerms = randomIntBetween(1, 10);
                List<Object> termsList = new ArrayList<>(numTerms);
                for (int t = 0; t < numTerms; t++) {
                    termsList.add(randomBoolean() ? values[randomIntBetween(0, numDocs - 1)] : randomLong());
                }
                Set<Integer> expectedTermsDocs = matchingDocIds(searcher, ft1.termsQuery(termsList, context));
                assertEquals("terms query " + termsList, expectedTermsDocs, matchingDocIds(searcher, ft2.termsQuery(termsList, context)));
                assertEquals("terms query " + termsList, expectedTermsDocs, matchingDocIds(searcher, ft3.termsQuery(termsList, context)));

                long boundA = randomLong();
                long boundB = randomLong();
                long lower = Math.min(boundA, boundB);
                long upper = Math.max(boundA, boundB);
                boolean includeLower = randomBoolean();
                boolean includeUpper = randomBoolean();
                String rangeDesc = (includeLower ? "[" : "(") + lower + "," + upper + (includeUpper ? "]" : ")");
                Set<Integer> expectedRangeDocs = matchingDocIds(
                    searcher,
                    ft1.rangeQuery(lower, upper, includeLower, includeUpper, context)
                );
                assertEquals(
                    "range query " + rangeDesc + " with doc values",
                    expectedRangeDocs,
                    matchingDocIds(searcher, ft2.rangeQuery(lower, upper, includeLower, includeUpper, context))
                );
                assertEquals(
                    "range query " + rangeDesc + " without doc values",
                    expectedRangeDocs,
                    matchingDocIds(searcher, ft3.rangeQuery(lower, upper, includeLower, includeUpper, context))
                );
            }
        });
    }

    public void testColumnarArrayOrderRoundTrip() throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.name()).build();
        DocumentMapper mapper = createMapperService(settings, mapping(b -> b.startObject("field").field("type", "long").endObject()))
            .documentMapper();
        long v1 = randomLong();
        long v2 = randomLong();
        long v3 = randomLong();
        // Out-of-order with v2 duplicated — sorted-deduped output would collapse the run.
        String src = syntheticSource(mapper, b -> b.array("field", v2, v1, v3, v2));
        assertThat(src, containsString("\"field\":[" + v2 + "," + v1 + "," + v3 + "," + v2 + "]"));
    }

}
