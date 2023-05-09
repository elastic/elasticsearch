/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
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

public class NumericTermsAggregatorTests extends AggregatorTestCase {
    private static final String LONG_FIELD = "long";

    private static final List<Long> dataset;
    static {
        List<Long> d = new ArrayList<>(45);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < i; j++) {
                d.add((long) i);
            }
        }
        dataset = d;
    }

    public void testMatchNoDocs() throws IOException {

        testSearchCase(
            new MatchNoDocsQuery(),
            dataset,
            aggregation -> aggregation.field(LONG_FIELD),
            agg -> assertEquals(0, agg.getBuckets().size()),
            null // without type hint
        );

        testSearchCase(
            new MatchNoDocsQuery(),
            dataset,
            aggregation -> aggregation.field(LONG_FIELD),
            agg -> assertEquals(0, agg.getBuckets().size()),
            ValueType.NUMERIC // with type hint
        );
    }

    public void testMatchAllDocs() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(query, dataset, aggregation -> aggregation.field(LONG_FIELD), agg -> {
            assertEquals(9, agg.getBuckets().size());
            for (int i = 0; i < 9; i++) {
                LongTerms.Bucket bucket = (LongTerms.Bucket) agg.getBuckets().get(i);
                assertThat(bucket.getKey(), equalTo(9L - i));
                assertThat(bucket.getDocCount(), equalTo(9L - i));
            }
        },
            null // without type hint
        );

        testSearchCase(query, dataset, aggregation -> aggregation.field(LONG_FIELD), agg -> {
            assertEquals(9, agg.getBuckets().size());
            for (int i = 0; i < 9; i++) {
                LongTerms.Bucket bucket = (LongTerms.Bucket) agg.getBuckets().get(i);
                assertThat(bucket.getKey(), equalTo(9L - i));
                assertThat(bucket.getDocCount(), equalTo(9L - i));
            }
        },
            ValueType.NUMERIC // with type hint
        );
    }

    public void testBadIncludeExclude() throws IOException {
        IncludeExclude includeExclude = new IncludeExclude("foo", null, null, null);

        // Numerics don't support any regex include/exclude, so should fail no matter what we do

        AggregationExecutionException e = expectThrows(
            AggregationExecutionException.class,
            () -> testSearchCase(
                new MatchNoDocsQuery(),
                dataset,
                aggregation -> aggregation.field(LONG_FIELD).includeExclude(includeExclude).format("yyyy-MM-dd"),
                agg -> fail("test should have failed with exception"),
                null
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "Aggregation [_name] cannot support regular expression style "
                    + "include/exclude settings as they can only be applied to string fields. Use an array of numeric "
                    + "values for include/exclude clauses used to filter numeric fields"
            )
        );

        e = expectThrows(
            AggregationExecutionException.class,
            () -> testSearchCase(
                new MatchNoDocsQuery(),
                dataset,
                aggregation -> aggregation.field(LONG_FIELD).includeExclude(includeExclude).format("yyyy-MM-dd"),
                agg -> fail("test should have failed with exception"),
                ValueType.NUMERIC // with type hint
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "Aggregation [_name] cannot support regular expression style "
                    + "include/exclude settings as they can only be applied to string fields. Use an array of numeric "
                    + "values for include/exclude clauses used to filter numeric fields"
            )
        );

    }

    private void testSearchCase(
        Query query,
        List<Long> dataset,
        Consumer<TermsAggregationBuilder> configure,
        Consumer<InternalMappedTerms<?, ?>> verify,
        ValueType valueType
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (Long value : dataset) {
                    document.add(new SortedNumericDocValuesField(LONG_FIELD, value));
                    document.add(new LongPoint(LONG_FIELD, value));
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name");
                if (valueType != null) {
                    aggregationBuilder.userValueTypeHint(valueType);
                }
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                MappedFieldType longFieldType = new NumberFieldMapper.NumberFieldType(LONG_FIELD, NumberFieldMapper.NumberType.LONG);

                InternalMappedTerms<?, ?> rareTerms = searchAndReduce(
                    indexSearcher,
                    new AggTestConfig(aggregationBuilder, longFieldType).withQuery(query)
                );
                verify.accept(rareTerms);
            }
        }
    }

}
