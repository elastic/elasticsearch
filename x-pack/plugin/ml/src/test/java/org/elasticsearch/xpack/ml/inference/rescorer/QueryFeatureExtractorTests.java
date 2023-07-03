/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.rescorer;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilder;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class QueryFeatureExtractorTests extends AbstractBuilderTestCase {

    private Directory dir;
    private IndexReader reader;
    private IndexSearcher searcher;

    private void addDocs(String[] textValues, int[] numberValues) throws IOException {
        dir = newDirectory();
        try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < textValues.length; i++) {
                Document doc = new Document();
                doc.add(newTextField(TEXT_FIELD_NAME, textValues[i], Field.Store.NO));
                doc.add(new IntField(INT_FIELD_NAME, numberValues[i], Field.Store.YES));
                indexWriter.addDocument(doc);
                if (randomBoolean()) {
                    indexWriter.flush();
                }
            }
            reader = indexWriter.getReader();
        }
        searcher = newSearcher(reader);
        searcher.setSimilarity(new ClassicSimilarity());
    }

    public void testQueryExtractor() throws IOException {
        addDocs(
            new String[] { "the quick brown fox", "the slow brown fox", "the grey dog", "yet another string" },
            new int[] { 5, 10, 12, 11 }
        );
        QueryRewriteContext ctx = createQueryRewriteContext();
        List<QueryExtractorBuilder> queryExtractorBuilders = List.of(
            new QueryExtractorBuilder("text_score", QueryProvider.fromParsedQuery(QueryBuilders.matchQuery(TEXT_FIELD_NAME, "quick fox")))
                .rewrite(ctx),
            new QueryExtractorBuilder(
                "number_score",
                QueryProvider.fromParsedQuery(QueryBuilders.rangeQuery(INT_FIELD_NAME).from(12).to(12))
            ).rewrite(ctx),
            new QueryExtractorBuilder(
                "matching_none",
                QueryProvider.fromParsedQuery(QueryBuilders.termQuery(TEXT_FIELD_NAME, "never found term"))
            ).rewrite(ctx),
            new QueryExtractorBuilder(
                "matching_missing_field",
                QueryProvider.fromParsedQuery(QueryBuilders.termQuery("missing_text", "quick fox"))
            ).rewrite(ctx)
        );
        SearchExecutionContext dummySEC = createSearchExecutionContext();
        List<Weight> weights = new ArrayList<>();
        List<String> featureNames = new ArrayList<>();
        for (QueryExtractorBuilder qeb : queryExtractorBuilders) {
            Query q = qeb.query().getParsedQuery().toQuery(dummySEC);
            Weight weight = searcher.rewrite(q).createWeight(searcher, ScoreMode.COMPLETE, 1f);
            weights.add(weight);
            featureNames.add(qeb.featureName());
        }
        QueryFeatureExtractor queryFeatureExtractor = new QueryFeatureExtractor(featureNames, weights);
        List<Map<String, Object>> extractedFeatures = new ArrayList<>();
        for (LeafReaderContext leafReaderContext : searcher.getLeafContexts()) {
            int maxDoc = leafReaderContext.reader().maxDoc();
            queryFeatureExtractor.setNextReader(leafReaderContext);
            for (int i = 0; i < maxDoc; i++) {
                Map<String, Object> featureMap = new HashMap<>();
                queryFeatureExtractor.addFeatures(featureMap, i);
                extractedFeatures.add(featureMap);
            }
        }
        assertThat(extractedFeatures, hasSize(4));
        // Should never add features for queries that don't match a document or on documents where the field is missing
        for (Map<String, Object> features : extractedFeatures) {
            assertThat(features, not(hasKey("matching_none")));
            assertThat(features, not(hasKey("matching_missing_field")));
        }
        // First two only match the text field
        assertThat(extractedFeatures.get(0), hasEntry("text_score", 1.7135582f));
        assertThat(extractedFeatures.get(0), not(hasKey("number_score")));
        assertThat(extractedFeatures.get(1), hasEntry("text_score", 0.7554128f));
        assertThat(extractedFeatures.get(1), not(hasKey("number_score")));
        // Only matches the range query
        assertThat(extractedFeatures.get(2), hasEntry("number_score", 1f));
        assertThat(extractedFeatures.get(2), not(hasKey("text_score")));
        // No query matches
        assertThat(extractedFeatures.get(3), anEmptyMap());
        reader.close();
        dir.close();
    }

}
