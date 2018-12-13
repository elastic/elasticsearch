/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SingleDimensionValuesSourceTests extends ESTestCase {
    public void testBinarySorted() {
        MappedFieldType keyword = new KeywordFieldMapper.KeywordFieldType();
        keyword.setName("keyword");
        BinaryValuesSource source = new BinaryValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            (b) -> {},
            keyword,
            context -> null,
            DocValueFormat.RAW,
            false,
            1,
            1
        );
        assertNull(source.createSortedDocsProducerOrNull(mockIndexReader(100, 49), null));
        IndexReader reader = mockIndexReader(1, 1);
        assertNotNull(source.createSortedDocsProducerOrNull(reader, new MatchAllDocsQuery()));
        assertNotNull(source.createSortedDocsProducerOrNull(reader, null));
        assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("foo", "bar"))));
        assertNull(source.createSortedDocsProducerOrNull(reader,
            new TermQuery(new Term("keyword", "toto)"))));

        source = new BinaryValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            (b) -> {},
            keyword,
            context -> null,
            DocValueFormat.RAW,
            true,
            1,
            1
        );
        assertNull(source.createSortedDocsProducerOrNull(reader, new MatchAllDocsQuery()));
        assertNull(source.createSortedDocsProducerOrNull(reader, null));

        source = new BinaryValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            (b) -> {},
            keyword,
            context -> null,
            DocValueFormat.RAW,
            false,
            0,
            -1
        );
        assertNull(source.createSortedDocsProducerOrNull(reader, null));

        MappedFieldType ip = new IpFieldMapper.IpFieldType();
        ip.setName("ip");
        source = new BinaryValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            (b) -> {},
            ip,
            context -> null,
            DocValueFormat.RAW,
            false,
            1,
            1);
        assertNull(source.createSortedDocsProducerOrNull(reader, null));
    }

    public void testGlobalOrdinalsSorted() {
        final MappedFieldType keyword = new KeywordFieldMapper.KeywordFieldType();
        keyword.setName("keyword");
        GlobalOrdinalValuesSource source = new GlobalOrdinalValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            keyword, context -> null,
            DocValueFormat.RAW,
            false,
            1,
            1
        );
        assertNull(source.createSortedDocsProducerOrNull(mockIndexReader(100, 49), null));
        IndexReader reader = mockIndexReader(1, 1);
        assertNotNull(source.createSortedDocsProducerOrNull(reader, new MatchAllDocsQuery()));
        assertNotNull(source.createSortedDocsProducerOrNull(reader, null));
        assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("foo", "bar"))));
        assertNull(source.createSortedDocsProducerOrNull(reader,
            new TermQuery(new Term("keyword", "toto)"))));

        source = new GlobalOrdinalValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            keyword,
            context -> null,
            DocValueFormat.RAW,
            true,
            1,
            1
        );
        assertNull(source.createSortedDocsProducerOrNull(reader, new MatchAllDocsQuery()));
        assertNull(source.createSortedDocsProducerOrNull(reader, null));
        assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("foo", "bar"))));

        source = new GlobalOrdinalValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            keyword,
            context -> null,
            DocValueFormat.RAW,
            false,
            1,
            -1
        );
        assertNull(source.createSortedDocsProducerOrNull(reader, null));
        assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("foo", "bar"))));

        final MappedFieldType ip = new IpFieldMapper.IpFieldType();
        ip.setName("ip");
        source = new GlobalOrdinalValuesSource(
            BigArrays.NON_RECYCLING_INSTANCE,
            ip,
            context -> null,
            DocValueFormat.RAW,
            false,
            1,
            1
        );
        assertNull(source.createSortedDocsProducerOrNull(reader, null));
        assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("foo", "bar"))));
    }

    public void testNumericSorted() {
        for (NumberFieldMapper.NumberType numberType : NumberFieldMapper.NumberType.values()) {
            MappedFieldType number = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
            number.setName("number");
            final SingleDimensionValuesSource<?> source;
            if (numberType == NumberFieldMapper.NumberType.BYTE ||
                    numberType == NumberFieldMapper.NumberType.SHORT ||
                    numberType == NumberFieldMapper.NumberType.INTEGER ||
                    numberType == NumberFieldMapper.NumberType.LONG) {

                source = new LongValuesSource(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    number,
                    context -> null,
                    value -> value,
                    DocValueFormat.RAW,
                    false,
                    1,
                    1
                );
                assertNull(source.createSortedDocsProducerOrNull(mockIndexReader(100, 49), null));
                IndexReader reader = mockIndexReader(1, 1);
                assertNotNull(source.createSortedDocsProducerOrNull(reader, new MatchAllDocsQuery()));
                assertNotNull(source.createSortedDocsProducerOrNull(reader, null));
                assertNotNull(source.createSortedDocsProducerOrNull(reader, LongPoint.newRangeQuery("number", 0, 1)));
                assertNotNull(source.createSortedDocsProducerOrNull(reader, new IndexOrDocValuesQuery(
                    LongPoint.newRangeQuery("number", 0, 1), new MatchAllDocsQuery())));
                assertNotNull(source.createSortedDocsProducerOrNull(reader, new DocValuesFieldExistsQuery("number")));
                assertNotNull(source.createSortedDocsProducerOrNull(reader,
                    new ConstantScoreQuery(new DocValuesFieldExistsQuery("number"))));
                assertNotNull(source.createSortedDocsProducerOrNull(reader, new BoostQuery(new IndexOrDocValuesQuery(
                    LongPoint.newRangeQuery("number", 0, 1), new MatchAllDocsQuery()), 2.0f)));
                assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("keyword", "toto)"))));

                LongValuesSource sourceWithMissing = new LongValuesSource(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    number,
                    context -> null,
                    value -> value,
                    DocValueFormat.RAW,
                    true,
                    1,
                    1);
                assertNull(sourceWithMissing.createSortedDocsProducerOrNull(reader, new MatchAllDocsQuery()));
                assertNull(sourceWithMissing.createSortedDocsProducerOrNull(reader, null));
                assertNull(sourceWithMissing.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("keyword", "toto)"))));
                assertNull(sourceWithMissing.createSortedDocsProducerOrNull(reader, new DocValuesFieldExistsQuery("number")));
                assertNull(sourceWithMissing.createSortedDocsProducerOrNull(reader,
                    new ConstantScoreQuery(new DocValuesFieldExistsQuery("number"))));

                LongValuesSource sourceRev = new LongValuesSource(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    number,
                    context -> null,
                    value -> value,
                    DocValueFormat.RAW,
                    false,
                    1,
                    -1
                );
                assertNull(sourceRev.createSortedDocsProducerOrNull(reader, null));
                assertNull(sourceRev.createSortedDocsProducerOrNull(reader, new DocValuesFieldExistsQuery("number")));
                assertNull(sourceRev.createSortedDocsProducerOrNull(reader,
                    new ConstantScoreQuery(new DocValuesFieldExistsQuery("number"))));
                assertNull(sourceWithMissing.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("keyword", "toto)"))));
            } else if (numberType == NumberFieldMapper.NumberType.HALF_FLOAT ||
                            numberType == NumberFieldMapper.NumberType.FLOAT ||
                            numberType == NumberFieldMapper.NumberType.DOUBLE) {
                source = new DoubleValuesSource(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    number,
                    context -> null,
                    DocValueFormat.RAW,
                    false,
                    1,
                    1
                );
                IndexReader reader = mockIndexReader(1, 1);
                assertNull(source.createSortedDocsProducerOrNull(reader, null));
                assertNull(source.createSortedDocsProducerOrNull(reader, new DocValuesFieldExistsQuery("number")));
                assertNull(source.createSortedDocsProducerOrNull(reader, new TermQuery(new Term("keyword", "toto)"))));
                assertNull(source.createSortedDocsProducerOrNull(reader,
                    new ConstantScoreQuery(new DocValuesFieldExistsQuery("number"))));
            } else{
                throw new AssertionError ("missing type:" + numberType.typeName());
            }
            assertNull(source.createSortedDocsProducerOrNull(mockIndexReader(100, 49), null));
        }
    }

    private static IndexReader mockIndexReader(int maxDoc, int numDocs) {
        IndexReader reader = mock(IndexReader.class);
        when(reader.hasDeletions()).thenReturn(maxDoc - numDocs > 0);
        when(reader.maxDoc()).thenReturn(maxDoc);
        when(reader.numDocs()).thenReturn(numDocs);
        return reader;
    }
}
