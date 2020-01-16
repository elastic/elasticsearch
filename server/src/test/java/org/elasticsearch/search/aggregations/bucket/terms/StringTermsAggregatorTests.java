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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class StringTermsAggregatorTests extends AggregatorTestCase {
    private static final String LONG_FIELD = "numeric";
    private static final String KEYWORD_FIELD = "keyword";

    private static final List<Long> dataset;
    static {
        List<Long> d = new ArrayList<>(45);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < i; j++) {
                d.add((long) i);
            }
        }
        dataset  = d;
    }

    public void testMatchNoDocs() throws IOException {
        testBothCases(new MatchNoDocsQuery(), dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD),
            agg -> assertEquals(0, agg.getBuckets().size()), ValueType.STRING
        );
        testBothCases(new MatchNoDocsQuery(), dataset,
            aggregation -> aggregation.field(LONG_FIELD),
            agg -> assertEquals(0, agg.getBuckets().size()), ValueType.NUMERIC
        );
    }

    public void testMatchAllDocs() throws IOException {
        Query query = new MatchAllDocsQuery();

        testBothCases(query, dataset,
            aggregation -> aggregation.field(LONG_FIELD),
            agg -> {
                assertEquals(9, agg.getBuckets().size());
                for (int i = 0; i < 9; i++) {
                    LongTerms.Bucket bucket = (LongTerms.Bucket) agg.getBuckets().get(i);
                    assertThat(bucket.getKey(), equalTo(9L - i));
                    assertThat(bucket.getDocCount(), equalTo(9L - i));
                }
            }, ValueType.NUMERIC
        );
        testBothCases(query, dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD),
            agg -> {
                assertEquals(9, agg.getBuckets().size());
                for (int i = 0; i < 9; i++) {
                    StringTerms.Bucket bucket = (StringTerms.Bucket) agg.getBuckets().get(i);
                    assertThat(bucket.getKey(), equalTo(String.valueOf(9L - i)));
                    assertThat(bucket.getDocCount(), equalTo(9L - i));
                }
            }, ValueType.STRING
        );
    }

    public void testBadIncludeExclude() throws IOException {
        IncludeExclude includeExclude = new IncludeExclude(new RegExp("foo"), null);

        // Bytes fails if the formatter is not RAW.  Note this is still on the long field, just hinted as string
        AggregationExecutionException e = expectThrows(AggregationExecutionException.class,
            () -> testBothCases(new MatchNoDocsQuery(), dataset,
                aggregation -> aggregation.field(LONG_FIELD).includeExclude(includeExclude).format("yyyy-MM-dd"),
                agg -> fail("test should have failed with exception"), ValueType.STRING
        ));
        assertThat(e.getMessage(), equalTo("Aggregation [_name] cannot support regular expression style " +
            "include/exclude settings as they can only be applied to string fields. Use an array of numeric " +
            "values for include/exclude clauses used to filter numeric fields"));

        // Numeric cannot use regex at all
        e = expectThrows(AggregationExecutionException.class, () -> testBothCases(new MatchNoDocsQuery(), dataset,
            aggregation -> aggregation.field(LONG_FIELD).includeExclude(includeExclude),
            agg -> fail("test should have failed with exception"), ValueType.NUMERIC
        ));
        assertThat(e.getMessage(), equalTo("Aggregation [_name] cannot support regular expression style include/exclude " +
            "settings as they can only be applied to string fields. Use an array of numeric values for include/exclude " +
            "clauses used to filter numeric fields"));
    }

    private void testSearchCase(Query query, List<Long> dataset,
                                Consumer<TermsAggregationBuilder> configure,
                                Consumer<InternalMappedTerms> verify, ValueType valueType) throws IOException {
        executeTestCase(false, query, dataset, configure, verify, valueType);
    }

    private void testSearchAndReduceCase(Query query, List<Long> dataset,
                                         Consumer<TermsAggregationBuilder> configure,
                                         Consumer<InternalMappedTerms> verify, ValueType valueType) throws IOException {
        executeTestCase(true, query, dataset, configure, verify, valueType);
    }

    private void testBothCases(Query query, List<Long> dataset,
                               Consumer<TermsAggregationBuilder> configure,
                               Consumer<InternalMappedTerms> verify, ValueType valueType) throws IOException {
        testSearchCase(query, dataset, configure, verify, valueType);
        testSearchAndReduceCase(query, dataset, configure, verify, valueType);
    }

    private void executeTestCase(boolean reduced, Query query, List<Long> dataset,
                                 Consumer<TermsAggregationBuilder> configure,
                                 Consumer<InternalMappedTerms> verify, ValueType valueType) throws IOException {

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (Long value : dataset) {
                    if (frequently()) {
                        indexWriter.commit();
                    }

                    document.add(new SortedNumericDocValuesField(LONG_FIELD, value));
                    document.add(new LongPoint(LONG_FIELD, value));
                    document.add(new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef(Long.toString(value))));
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name");
                aggregationBuilder.userValueTypeHint(valueType);
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                MappedFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType();
                keywordFieldType.setName(KEYWORD_FIELD);
                keywordFieldType.setHasDocValues(true);

                MappedFieldType longFieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                longFieldType.setName(LONG_FIELD);
                longFieldType.setHasDocValues(true);

                InternalMappedTerms rareTerms;
                if (reduced) {
                    rareTerms = searchAndReduce(indexSearcher, query, aggregationBuilder, keywordFieldType, longFieldType);
                } else {
                    rareTerms = search(indexSearcher, query, aggregationBuilder, keywordFieldType, longFieldType);
                }
                verify.accept(rareTerms);
            }
        }
    }

}
