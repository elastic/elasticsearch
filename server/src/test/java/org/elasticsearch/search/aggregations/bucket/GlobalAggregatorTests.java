/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregator;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.metrics.InternalMin;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;

import java.io.IOException;
import java.util.function.BiConsumer;

import static java.util.Collections.singleton;

public class GlobalAggregatorTests extends AggregatorTestCase {
    public void testNoDocs() throws IOException {
        testCase(iw -> {
            // Intentionally not writing any docs
        }, (global, min) -> {
            assertEquals(0, global.getDocCount());
            assertEquals(Double.POSITIVE_INFINITY, min.getValue(), 0);
        });
    }

    public void testSomeDocs() throws IOException {
        testCase(iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 1)));
        }, (global, min) -> {
            assertEquals(2, global.getDocCount());
            assertEquals(1, min.getValue(), 0);
        });
    }

    // Note that `global`'s fancy support for ignoring the query comes from special code in AggregationPhase. We don't test that here.

    private void testCase(CheckedConsumer<RandomIndexWriter, IOException> buildIndex, BiConsumer<InternalGlobal, InternalMin> verify)
            throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        GlobalAggregationBuilder aggregationBuilder = new GlobalAggregationBuilder("_name");
        aggregationBuilder.subAggregation(new MinAggregationBuilder("in_global").field("number"));
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);

        GlobalAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        InternalGlobal result = (InternalGlobal) aggregator.buildTopLevel();
        verify.accept(result, (InternalMin) result.getAggregations().asMap().get("in_global"));

        indexReader.close();
        directory.close();
    }
}
