/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class BinaryTermsAggregatorTests extends AggregatorTestCase {
    private static final String BINARY_FIELD = "binary";

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
            aggregation -> aggregation.field(BINARY_FIELD),
            agg -> assertEquals(0, agg.getBuckets().size()),
            ValueType.STRING
        );
    }

    public void testMatchAllDocs() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(query, dataset, aggregation -> aggregation.field(BINARY_FIELD), agg -> {
            assertEquals(9, agg.getBuckets().size());
            for (int i = 0; i < 9; i++) {
                StringTerms.Bucket bucket = (StringTerms.Bucket) agg.getBuckets().get(i);
                byte[] bytes = Numbers.longToBytes(9L - i);
                String bytesAsString = (String) DocValueFormat.BINARY.format(new BytesRef(bytes));
                assertThat(bucket.getKey(), equalTo(bytesAsString));
                assertThat(bucket.getDocCount(), equalTo(9L - i));
            }
        }, null);
    }

    public void testBadIncludeExclude() throws IOException {
        IncludeExclude includeExclude = new IncludeExclude("foo", null, null, null);

        // Make sure the include/exclude fails regardless of how the user tries to type hint the agg
        AggregationExecutionException e = expectThrows(
            AggregationExecutionException.class,
            () -> testSearchCase(
                new MatchNoDocsQuery(),
                dataset,
                aggregation -> aggregation.field(BINARY_FIELD).includeExclude(includeExclude).format("yyyy-MM-dd"),
                agg -> fail("test should have failed with exception"),
                null // default, no hint
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "Aggregation [_name] cannot support regular expression style include/exclude settings as "
                    + "they can only be applied to string fields. Use an array of values for include/exclude clauses"
            )
        );

        e = expectThrows(
            AggregationExecutionException.class,
            () -> testSearchCase(
                new MatchNoDocsQuery(),
                dataset,
                aggregation -> aggregation.field(BINARY_FIELD).includeExclude(includeExclude).format("yyyy-MM-dd"),
                agg -> fail("test should have failed with exception"),
                ValueType.STRING // string type hint
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "Aggregation [_name] cannot support regular expression style include/exclude settings as "
                    + "they can only be applied to string fields. Use an array of values for include/exclude clauses"
            )
        );
    }

    public void testBadUserValueTypeHint() throws IOException {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testSearchCase(
                new MatchNoDocsQuery(),
                dataset,
                aggregation -> aggregation.field(BINARY_FIELD),
                agg -> fail("test should have failed with exception"),
                ValueType.NUMERIC // numeric type hint
            )
        );
        assertThat(e.getMessage(), equalTo("Field type [binary] is incompatible with specified value_type [numeric]"));
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
                    document.add(new BinaryFieldMapper.CustomBinaryDocValuesField(BINARY_FIELD, Numbers.longToBytes(value)));
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name");
                if (valueType != null) {
                    aggregationBuilder.userValueTypeHint(valueType);
                }
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                MappedFieldType binaryFieldType = new BinaryFieldMapper.BinaryFieldType(BINARY_FIELD);

                InternalMappedTerms<?, ?> rareTerms = searchAndReduce(indexSearcher, query, aggregationBuilder, binaryFieldType);
                verify.accept(rareTerms);
            }
        }
    }

}
