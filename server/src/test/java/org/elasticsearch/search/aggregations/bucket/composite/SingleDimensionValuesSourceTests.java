/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SingleDimensionValuesSourceTests extends ESTestCase {
    public void testBinarySorted() {
        MappedFieldType keyword = new KeywordFieldMapper.KeywordFieldType("keyword");
        BinaryValuesSource source = new BinaryValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            (b) -> {},
            keyword,
            context -> null,
            DocValueFormat.RAW,
            false,
            MissingOrder.DEFAULT,
            1,
            1
        );
        assertNull(source.createSortedDocsProducerOrNull(mockIndexReader(100, 49), null));
        IndexReader reader = mockIndexReader(1, 1);
        assertNotNull(source.createSortedDocsProducerOrNull(reader, Queries.ALL_DOCS_INSTANCE));
        assertNotNull(source.createSortedDocsProducerOrNull(reader, null));
        assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("foo", "bar"))));
        assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("keyword", "toto)"))));

        source = new BinaryValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            (b) -> {},
            keyword,
            context -> null,
            DocValueFormat.RAW,
            true,
            MissingOrder.DEFAULT,
            1,
            1
        );
        assertNull(source.createSortedDocsProducerOrNull(reader, Queries.ALL_DOCS_INSTANCE));
        assertNull(source.createSortedDocsProducerOrNull(reader, null));

        source = new BinaryValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            (b) -> {},
            keyword,
            context -> null,
            DocValueFormat.RAW,
            false,
            MissingOrder.DEFAULT,
            0,
            -1
        );
        assertNull(source.createSortedDocsProducerOrNull(reader, null));

        MappedFieldType ip = new IpFieldMapper.IpFieldType("ip");
        source = new BinaryValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            (b) -> {},
            ip,
            context -> null,
            DocValueFormat.RAW,
            false,
            MissingOrder.DEFAULT,
            1,
            1
        );
        assertNull(source.createSortedDocsProducerOrNull(reader, null));
    }

    public void testGlobalOrdinalsSorted() {
        final MappedFieldType keyword = new KeywordFieldMapper.KeywordFieldType("keyword");
        GlobalOrdinalValuesSource source = new GlobalOrdinalValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            keyword,
            0L,
            context -> null,
            DocValueFormat.RAW,
            false,
            MissingOrder.DEFAULT,
            1,
            1
        );
        assertNull(source.createSortedDocsProducerOrNull(mockIndexReader(100, 49), null));
        IndexReader reader = mockIndexReader(1, 1);
        assertNotNull(source.createSortedDocsProducerOrNull(reader, Queries.ALL_DOCS_INSTANCE));
        assertNotNull(source.createSortedDocsProducerOrNull(reader, null));
        assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("foo", "bar"))));
        assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("keyword", "toto)"))));

        source = new GlobalOrdinalValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            keyword,
            0L,
            context -> null,
            DocValueFormat.RAW,
            true,
            MissingOrder.DEFAULT,
            1,
            1
        );
        assertNull(source.createSortedDocsProducerOrNull(reader, Queries.ALL_DOCS_INSTANCE));
        assertNull(source.createSortedDocsProducerOrNull(reader, null));
        assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("foo", "bar"))));

        source = new GlobalOrdinalValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            keyword,
            0L,
            context -> null,
            DocValueFormat.RAW,
            false,
            MissingOrder.DEFAULT,
            1,
            -1
        );
        assertNull(source.createSortedDocsProducerOrNull(reader, null));
        assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("foo", "bar"))));

        final MappedFieldType ip = new IpFieldMapper.IpFieldType("ip");
        source = new GlobalOrdinalValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            ip,
            0L,
            context -> null,
            DocValueFormat.RAW,
            false,
            MissingOrder.DEFAULT,
            1,
            1
        );
        assertNull(source.createSortedDocsProducerOrNull(reader, null));
        assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("foo", "bar"))));
    }

    public void testNumericSorted() {
        for (NumberFieldMapper.NumberType numberType : NumberFieldMapper.NumberType.values()) {
            MappedFieldType number = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
            final SingleDimensionValuesSource<?> source;
            if (numberType == NumberFieldMapper.NumberType.BYTE
                || numberType == NumberFieldMapper.NumberType.SHORT
                || numberType == NumberFieldMapper.NumberType.INTEGER
                || numberType == NumberFieldMapper.NumberType.LONG) {

                source = new LongValuesSource(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    number,
                    context -> null,
                    value -> value,
                    DocValueFormat.RAW,
                    false,
                    MissingOrder.DEFAULT,
                    1,
                    1
                );
                assertNull(source.createSortedDocsProducerOrNull(mockIndexReader(100, 49), null));
                IndexReader reader = mockIndexReader(1, 1);
                assertNotNull(source.createSortedDocsProducerOrNull(reader, Queries.ALL_DOCS_INSTANCE));
                assertNotNull(source.createSortedDocsProducerOrNull(reader, null));
                assertNotNull(source.createSortedDocsProducerOrNull(reader, LongPoint.newRangeQuery("number", 0, 1)));
                assertNotNull(
                    source.createSortedDocsProducerOrNull(
                        reader,
                        new IndexOrDocValuesQuery(LongPoint.newRangeQuery("number", 0, 1), Queries.ALL_DOCS_INSTANCE)
                    )
                );
                assertNotNull(source.createSortedDocsProducerOrNull(reader, new FieldExistsQuery("number")));
                assertNotNull(source.createSortedDocsProducerOrNull(reader, new ConstantScoreQuery(new FieldExistsQuery("number"))));
                assertNotNull(
                    source.createSortedDocsProducerOrNull(
                        reader,
                        new BoostQuery(new IndexOrDocValuesQuery(LongPoint.newRangeQuery("number", 0, 1), Queries.ALL_DOCS_INSTANCE), 2.0f)
                    )
                );
                assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("keyword", "toto)"))));

                LongValuesSource sourceWithMissing = new LongValuesSource(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    number,
                    context -> null,
                    value -> value,
                    DocValueFormat.RAW,
                    true,
                    MissingOrder.DEFAULT,
                    1,
                    1
                );
                assertNull(sourceWithMissing.createSortedDocsProducerOrNull(reader, Queries.ALL_DOCS_INSTANCE));
                assertNull(sourceWithMissing.createSortedDocsProducerOrNull(reader, null));
                assertNull(sourceWithMissing.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("keyword", "toto)"))));
                assertNull(sourceWithMissing.createSortedDocsProducerOrNull(reader, new FieldExistsQuery("number")));
                assertNull(
                    sourceWithMissing.createSortedDocsProducerOrNull(reader, new ConstantScoreQuery(new FieldExistsQuery("number")))
                );

                LongValuesSource sourceRev = new LongValuesSource(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    number,
                    context -> null,
                    value -> value,
                    DocValueFormat.RAW,
                    false,
                    MissingOrder.DEFAULT,
                    1,
                    -1
                );
                assertNull(sourceRev.createSortedDocsProducerOrNull(reader, null));
                assertNull(sourceRev.createSortedDocsProducerOrNull(reader, new FieldExistsQuery("number")));
                assertNull(sourceRev.createSortedDocsProducerOrNull(reader, new ConstantScoreQuery(new FieldExistsQuery("number"))));
                assertNull(sourceWithMissing.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("keyword", "toto)"))));
            } else if (numberType == NumberFieldMapper.NumberType.HALF_FLOAT
                || numberType == NumberFieldMapper.NumberType.FLOAT
                || numberType == NumberFieldMapper.NumberType.DOUBLE) {
                    source = new DoubleValuesSource(
                        BigArrays.NON_RECYCLING_INSTANCE,
                        number,
                        context -> null,
                        DocValueFormat.RAW,
                        false,
                        MissingOrder.DEFAULT,
                        1,
                        1
                    );
                    IndexReader reader = mockIndexReader(1, 1);
                    assertNull(source.createSortedDocsProducerOrNull(reader, null));
                    assertNull(source.createSortedDocsProducerOrNull(reader, new FieldExistsQuery("number")));
                    assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("keyword", "toto)"))));
                    assertNull(source.createSortedDocsProducerOrNull(reader, new ConstantScoreQuery(new FieldExistsQuery("number"))));
                } else {
                    throw new AssertionError("missing type:" + numberType.typeName());
                }
            assertNull(source.createSortedDocsProducerOrNull(mockIndexReader(100, 49), null));
        }
    }

    /**
     * A field that is only backed by a doc values skipper has no BKD tree, so building a
     * {@link PointsSortedDocsProducer} for it would silently collect nothing. This is the shape that
     * {@code @timestamp} takes in a logsdb or tsdb index that sorts on it, and it regressed when
     * {@code IndexType#supportsSortShortcuts} started accepting doc values skippers.
     */
    public void testSkipperBackedFieldsDoNotProduceAPointsProducer() {
        IndexReader reader = mockIndexReader(1, 1);

        MappedFieldType skipperDate = dateFieldType("@timestamp", IndexType.skippers());
        assertNull(longSource(skipperDate).createSortedDocsProducerOrNull(reader, null));
        assertNull(longSource(skipperDate).createSortedDocsProducerOrNull(reader, Queries.ALL_DOCS_INSTANCE));
        assertNull(longSource(skipperDate).createSortedDocsProducerOrNull(reader, new FieldExistsQuery("@timestamp")));

        MappedFieldType skipperLong = new NumberFieldMapper.NumberFieldType(
            "number",
            NumberFieldMapper.NumberType.LONG,
            IndexType.skippers(),
            false,
            false,
            null,
            Collections.emptyMap(),
            null,
            false,
            null,
            null,
            false,
            false,
            false
        );
        assertNull(longSource(skipperLong).createSortedDocsProducerOrNull(reader, null));
        assertNull(longSource(skipperLong).createSortedDocsProducerOrNull(reader, Queries.ALL_DOCS_INSTANCE));

        // an [index_terms] numeric has a dense index, but it is an inverted index rather than a BKD tree
        MappedFieldType indexTermsLong = new NumberFieldMapper.NumberFieldType(
            "number",
            NumberFieldMapper.NumberType.LONG,
            IndexType.terms(true, true),
            false,
            false,
            null,
            Collections.emptyMap(),
            null,
            false,
            null,
            null,
            false,
            false,
            true
        );
        assertTrue(indexTermsLong.indexType().hasDenseIndex());
        assertFalse(indexTermsLong.indexType().hasPoints());
        assertNull(longSource(indexTermsLong).createSortedDocsProducerOrNull(reader, null));

        // a legacy archived date exposes points metadata derived from doc values, but still has no BKD tree
        MappedFieldType archivedDate = dateFieldType("@timestamp", IndexType.archivedPoints());
        assertTrue(archivedDate.indexType().hasPointsMetadata());
        assertFalse(archivedDate.indexType().hasPoints());
        assertNull(longSource(archivedDate).createSortedDocsProducerOrNull(reader, null));

        // the same fields keep the optimization when they do have a points index
        MappedFieldType indexedDate = new DateFieldMapper.DateFieldType("@timestamp");
        assertNotNull(longSource(indexedDate).createSortedDocsProducerOrNull(reader, null));
        MappedFieldType indexedLong = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        assertNotNull(longSource(indexedLong).createSortedDocsProducerOrNull(reader, null));
    }

    /**
     * The terms equivalent of {@link #testSkipperBackedFieldsDoNotProduceAPointsProducer()}. A keyword that is
     * only backed by a doc values skipper has no terms dictionary, so {@link TermsSortedDocsProducer} finds a
     * null {@link org.apache.lucene.index.Terms} and collects nothing. This is the shape that {@code host.name}
     * takes in logsdb, and that any {@code index: false} keyword takes once doc values skippers are enabled.
     */
    public void testSkipperBackedKeywordDoesNotProduceATermsProducer() {
        IndexReader reader = mockIndexReader(1, 1);
        MappedFieldType skipperKeyword = skipperBackedKeyword("keyword");

        assertNull(globalOrdinalsSource(skipperKeyword).createSortedDocsProducerOrNull(reader, null));
        assertNull(globalOrdinalsSource(skipperKeyword).createSortedDocsProducerOrNull(reader, Queries.ALL_DOCS_INSTANCE));
        assertNull(binarySource(skipperKeyword).createSortedDocsProducerOrNull(reader, null));
        assertNull(binarySource(skipperKeyword).createSortedDocsProducerOrNull(reader, Queries.ALL_DOCS_INSTANCE));
    }

    /**
     * An indexed keyword keeps the terms optimization, so the assertions above are about the missing terms
     * dictionary rather than about keywords in general. This guards against the terms fix over-reaching.
     */
    public void testIndexedKeywordStillProducesATermsProducer() {
        IndexReader reader = mockIndexReader(1, 1);
        MappedFieldType keyword = new KeywordFieldMapper.KeywordFieldType("keyword");

        assertNotNull(globalOrdinalsSource(keyword).createSortedDocsProducerOrNull(reader, null));
        assertNotNull(binarySource(keyword).createSortedDocsProducerOrNull(reader, null));
    }

    /**
     * {@code missing_bucket: true} disables the optimization only while there is no after key, so on an affected
     * build it masks the defect on the first page of a composite aggregation and then returns nothing once the
     * caller pages with the after key it was just handed. The skipper check has to hold in both states.
     */
    public void testSkipperBackedDateIsRejectedWhenPaginatingWithMissingBucket() {
        IndexReader reader = mockIndexReader(1, 1);
        MappedFieldType skipperDate = dateFieldType("@timestamp", IndexType.skippers());
        MappedFieldType indexedDate = new DateFieldMapper.DateFieldType("@timestamp");

        // with no after key, missing_bucket alone is enough to skip the optimization for either field
        assertNull(longSource(skipperDate, true).createSortedDocsProducerOrNull(reader, null));
        assertNull(longSource(indexedDate, true).createSortedDocsProducerOrNull(reader, null));

        // an after key re-enables it, so only the missing points index keeps the skipper backed field out
        LongValuesSource skipperWithAfter = longSource(skipperDate, true);
        skipperWithAfter.setAfter(100L);
        assertNull(skipperWithAfter.createSortedDocsProducerOrNull(reader, null));

        LongValuesSource indexedWithAfter = longSource(indexedDate, true);
        indexedWithAfter.setAfter(100L);
        assertNotNull(indexedWithAfter.createSortedDocsProducerOrNull(reader, null));
    }

    private static MappedFieldType dateFieldType(String name, IndexType indexType) {
        return new DateFieldMapper.DateFieldType(
            name,
            indexType,
            false,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            null,
            Collections.emptyMap()
        );
    }

    /**
     * Builds a keyword field type with doc values and a doc values skipper but no inverted index, which is what
     * {@code index: false} produces once {@code index.mapping.use_doc_values_skipper} is on.
     */
    private static MappedFieldType skipperBackedKeyword(String name) {
        FieldType fieldType = new FieldType();
        fieldType.setIndexOptions(IndexOptions.NONE);
        fieldType.setDocValuesType(DocValuesType.SORTED_SET);
        fieldType.setDocValuesSkipIndexType(DocValuesSkipIndexType.RANGE);
        fieldType.freeze();
        MappedFieldType keyword = new KeywordFieldMapper.KeywordFieldType(name, fieldType, false);
        assertTrue(keyword.indexType().hasDocValuesSkipper());
        assertFalse(keyword.indexType().hasTerms());
        return keyword;
    }

    private static LongValuesSource longSource(MappedFieldType fieldType) {
        return longSource(fieldType, false);
    }

    private static LongValuesSource longSource(MappedFieldType fieldType, boolean missingBucket) {
        return new LongValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            fieldType,
            context -> null,
            value -> value,
            DocValueFormat.RAW,
            missingBucket,
            MissingOrder.DEFAULT,
            1,
            1
        );
    }

    private static GlobalOrdinalValuesSource globalOrdinalsSource(MappedFieldType fieldType) {
        return new GlobalOrdinalValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            fieldType,
            0L,
            context -> null,
            DocValueFormat.RAW,
            false,
            MissingOrder.DEFAULT,
            1,
            1
        );
    }

    private static BinaryValuesSource binarySource(MappedFieldType fieldType) {
        return new BinaryValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            breaker -> {},
            fieldType,
            context -> null,
            DocValueFormat.RAW,
            false,
            MissingOrder.DEFAULT,
            1,
            1
        );
    }

    private static IndexReader mockIndexReader(int maxDoc, int numDocs) {
        IndexReader reader = mock(LeafReader.class);
        when(reader.hasDeletions()).thenReturn(maxDoc - numDocs > 0);
        when(reader.maxDoc()).thenReturn(maxDoc);
        when(reader.numDocs()).thenReturn(numDocs);
        return reader;
    }
}
