/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
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
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class KeywordTermsAggregatorTests extends AggregatorTestCase {
    private static final String KEYWORD_FIELD = "keyword";

    private static final List<String> dataset;
    static {
        List<String> d = new ArrayList<>(45);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < i; j++) {
                d.add(String.valueOf(i));
            }
        }
        dataset  = d;
    }

    public void testMatchNoDocs() throws IOException {
        testSearchCase(new MatchNoDocsQuery(), dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD),
            agg -> assertEquals(0, agg.getBuckets().size()), null // without type hint
        );

        testSearchCase(new MatchNoDocsQuery(), dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD),
            agg -> assertEquals(0, agg.getBuckets().size()), ValueType.STRING // with type hint
        );
    }

    public void testMatchAllDocs() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(query, dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD),
            agg -> {
                assertEquals(9, agg.getBuckets().size());
                for (int i = 0; i < 9; i++) {
                    StringTerms.Bucket bucket = (StringTerms.Bucket) agg.getBuckets().get(i);
                    assertThat(bucket.getKey(), equalTo(String.valueOf(9L - i)));
                    assertThat(bucket.getDocCount(), equalTo(9L - i));
                }
            }, null // without type hint
        );

        testSearchCase(query, dataset,
            aggregation -> aggregation.field(KEYWORD_FIELD),
            agg -> {
                assertEquals(9, agg.getBuckets().size());
                for (int i = 0; i < 9; i++) {
                    StringTerms.Bucket bucket = (StringTerms.Bucket) agg.getBuckets().get(i);
                    assertThat(bucket.getKey(), equalTo(String.valueOf(9L - i)));
                    assertThat(bucket.getDocCount(), equalTo(9L - i));
                }
            }, ValueType.STRING // with type hint
        );
    }

    private void testSearchCase(Query query, List<String> dataset,
                                Consumer<TermsAggregationBuilder> configure,
                                Consumer<InternalMappedTerms<?, ?>> verify, ValueType valueType) throws IOException {
        MappedFieldType keywordFieldType
            = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD, randomBoolean(), true, Collections.emptyMap());
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (String value : dataset) {
                    document.add(new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef(value)));
                    if (keywordFieldType.isSearchable()) {
                        document.add(new Field(KEYWORD_FIELD, new BytesRef(value), KeywordFieldMapper.Defaults.FIELD_TYPE));
                    }
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


                InternalMappedTerms<?, ?> rareTerms = searchAndReduce(indexSearcher, query, aggregationBuilder, keywordFieldType);
                verify.accept(rareTerms);
            }
        }
    }

}
