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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.containsString;

public class IntegerFieldMapperTests extends WholeNumberFieldMapperTests {

    @Override
    protected Number missingValue() {
        return 123;
    }

    @Override
    protected List<NumberTypeOutOfRangeSpec> outOfRangeSpecs() {
        return List.of(
            NumberTypeOutOfRangeSpec.of(NumberType.INTEGER, "2147483648", "is out of range for an integer"),
            NumberTypeOutOfRangeSpec.of(NumberType.INTEGER, "-2147483649", "is out of range for an integer"),
            NumberTypeOutOfRangeSpec.of(NumberType.INTEGER, 2147483648L, " out of range of int"),
            NumberTypeOutOfRangeSpec.of(NumberType.INTEGER, -2147483649L, " out of range of int")
        );
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        super.registerParameters(checker);
        checker.registerConflictCheck("index_terms", b -> b.field("index_terms", true));
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "integer");
    }

    @Override
    protected Number randomNumber() {
        if (randomBoolean()) {
            return randomInt();
        }
        if (randomBoolean()) {
            return randomDouble();
        }
        return randomDoubleBetween(Integer.MIN_VALUE, Integer.MAX_VALUE, true);
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    protected boolean supportsBulkIntBlockReading() {
        return true;
    }

    @Override
    protected Object[] getThreeSampleValues() {
        return new Object[] { 1, 2, 3 };
    }

    public void testIndexTermsIndexesSortableBytesTerms() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "integer");
            b.field("index_terms", true);
        }));

        int value = randomIntBetween(0, Integer.MAX_VALUE);
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", value)));
        List<IndexableField> fields = doc.rootDoc().getFields("field");

        // Should have a terms field (inverted index) with the sortable-bytes encoded value
        long termsCount = fields.stream().filter(f -> f.fieldType().indexOptions().compareTo(IndexOptions.NONE) > 0).count();
        assertEquals(1, termsCount);
        IndexableField termsField = fields.stream()
            .filter(f -> f.fieldType().indexOptions().compareTo(IndexOptions.NONE) > 0)
            .findFirst()
            .get();
        byte[] expected = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(value, expected, 0);
        assertEquals(new BytesRef(expected), termsField.binaryValue());

        // Should have doc values
        long dvCount = fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).count();
        assertEquals(1, dvCount);
        IndexableField dvField = fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).findFirst().get();
        assertEquals(value, dvField.numericValue().intValue());
    }

    public void testIndexTermsIndexesNegativeValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "integer");
            b.field("index_terms", true);
        }));

        int value = randomIntBetween(Integer.MIN_VALUE, -1);
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", value)));
        List<IndexableField> fields = doc.rootDoc().getFields("field");

        IndexableField termsField = fields.stream()
            .filter(f -> f.fieldType().indexOptions().compareTo(IndexOptions.NONE) > 0)
            .findFirst()
            .get();
        byte[] expected = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(value, expected, 0);
        assertEquals(new BytesRef(expected), termsField.binaryValue());

        IndexableField dvField = fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).findFirst().get();
        assertEquals(value, dvField.numericValue().intValue());
    }

    public void testIndexTermsOnlyAllowedOnInteger() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "long");
            b.field("index_terms", true);
        })));
        assertThat(e.getMessage(), containsString("[index_terms] is only supported on [integer] fields"));
    }

    public void testIndexTermsRequiresIndex() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "integer");
            b.field("index_terms", true);
            b.field("index", false);
        })));
        assertThat(e.getMessage(), containsString("[index_terms] requires that [index] is true"));
    }

    public void testIndexTermsRejectedOnLegacyIndex() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(IndexVersion.fromId(5000099), fieldMapping(b -> {
            b.field("type", "integer");
            b.field("index_terms", true);
        })));
        assertThat(e.getMessage(), containsString("[index_terms] is not supported on legacy indices"));
    }

    public void testIndexTermsRangeQueryWithoutDocValues() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "integer");
            b.field("index_terms", true);
            b.field("doc_values", false);
        }));
        NumberFieldMapper.NumberFieldType ft = (NumberFieldMapper.NumberFieldType) mapperService.fieldType("field");
        ParsedDocument doc42 = mapperService.documentMapper().parse(source(b -> b.field("field", 42)));
        ParsedDocument docNeg5 = mapperService.documentMapper().parse(source(b -> b.field("field", -5)));
        ParsedDocument doc100 = mapperService.documentMapper().parse(source(b -> b.field("field", 100)));
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(doc42.rootDoc());
            iw.addDocument(docNeg5.rootDoc());
            iw.addDocument(doc100.rootDoc());
        }, ir -> {
            IndexSearcher searcher = newSearcher(ir);
            Query query = ft.rangeQuery(-10, 50, true, true, createSearchExecutionContext(mapperService));
            assertEquals(2, searcher.count(query));
        });
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
     * Indexes the same random integer values into a points-based field and two index_terms
     * fields (one with doc values, one without), across a random number of segments, and asserts
     * that term, terms and range queries match the exact same set of documents on all three.
     */
    public void testIndexTermsMatchesPointsRandomized() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field1").field("type", "integer").endObject();
            b.startObject("field2").field("type", "integer").field("index_terms", true).endObject();
            b.startObject("field3").field("type", "integer").field("index_terms", true).field("doc_values", false).endObject();
        }));

        int numDocs = randomIntBetween(500, 1000);
        int[] values = new int[numDocs];
        for (int i = 0; i < numDocs; i++) {
            values[i] = randomInt();
        }

        withLuceneIndex(mapperService, iw -> {
            for (int value : values) {
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
                int termValue = randomBoolean() ? values[randomIntBetween(0, numDocs - 1)] : randomInt();
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
                    termsList.add(randomBoolean() ? values[randomIntBetween(0, numDocs - 1)] : randomInt());
                }
                Set<Integer> expectedTermsDocs = matchingDocIds(searcher, ft1.termsQuery(termsList, context));
                assertEquals("terms query " + termsList, expectedTermsDocs, matchingDocIds(searcher, ft2.termsQuery(termsList, context)));
                assertEquals("terms query " + termsList, expectedTermsDocs, matchingDocIds(searcher, ft3.termsQuery(termsList, context)));

                int boundA = randomInt();
                int boundB = randomInt();
                int lower = Math.min(boundA, boundB);
                int upper = Math.max(boundA, boundB);
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
}
