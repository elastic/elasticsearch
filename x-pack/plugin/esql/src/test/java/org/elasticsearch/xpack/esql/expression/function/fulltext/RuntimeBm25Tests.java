/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The runtime BM25 score of a row must be the score Lucene gives the same text in an index holding exactly the scored rows,
 * with the similarity indexed text fields default to. Lucene is the reference: it indexes the corpus, and the runtime side
 * computes the statistics the way the stats pass does (only rows with tokens count as documents).
 */
public class RuntimeBm25Tests extends ESTestCase {
    private static final List<String> VOCABULARY = List.of("fox", "dog", "quick", "lazy", "brown", "jumps", "over", "the", "cat", "moon");

    public void testScoresLikeAnIndexedTextField() throws IOException {
        List<String> docs = randomList(1, 40, () -> randomText(0, 30));
        List<String> queryTokens = randomList(1, 5, () -> randomFrom(VOCABULARY));
        Analyzer analyzer = new StandardAnalyzer();

        Map<Integer, Float> expected = luceneScores(analyzer, docs, queryTokens);

        Map<BytesRef, Integer> queryTerms = RuntimeBm25.analyzeQuery(analyzer, String.join(" ", queryTokens));
        List<BytesRef> terms = new ArrayList<>(queryTerms.keySet());
        int[] weights = terms.stream().mapToInt(queryTerms::get).toArray();
        Map<BytesRef, Integer> termIndex = new HashMap<>();
        for (int i = 0; i < terms.size(); i++) {
            termIndex.put(terms.get(i), i);
        }

        List<RuntimeBm25.RowStats> rows = new ArrayList<>();
        long docCount = 0;
        long sumLength = 0;
        long[] docFreqs = new long[terms.size()];
        for (String doc : docs) {
            RuntimeBm25.RowStats row = new RuntimeBm25.RowStats(terms.size());
            row.add(analyzer, doc, termIndex);
            rows.add(row);
            if (row.length > 0) {
                docCount++;
                sumLength += row.length;
            }
            for (int i = 0; i < terms.size(); i++) {
                docFreqs[i] += row.termFreqs[i] > 0 ? 1 : 0;
            }
        }
        SimScorer[] scorers = RuntimeBm25.scorers(docCount, sumLength, docFreqs, terms);

        for (int d = 0; d < docs.size(); d++) {
            double actual = scorers == null ? 0.0 : RuntimeBm25.score(rows.get(d), weights, scorers);
            float luceneScore = expected.getOrDefault(d, 0f);
            assertEquals("doc [" + docs.get(d) + "] for query " + queryTokens, luceneScore, actual, 1e-5 * Math.max(1, luceneScore));
        }
    }

    private static String randomText(int minTokens, int maxTokens) {
        return String.join(" ", randomList(minTokens, maxTokens, () -> randomFrom(VOCABULARY)));
    }

    /** Each doc's score for the query, by its position in {@code docs}; docs that don't match are absent. */
    private static Map<Integer, Float> luceneScores(Analyzer analyzer, List<String> docs, List<String> queryTokens) throws IOException {
        try (ByteBuffersDirectory directory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(analyzer).setSimilarity(RuntimeBm25.SIMILARITY);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (int d = 0; d < docs.size(); d++) {
                    Document document = new Document();
                    document.add(new TextField("f", docs.get(d), Field.Store.NO));
                    document.add(new StoredField("position", d));
                    writer.addDocument(document);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setSimilarity(RuntimeBm25.SIMILARITY);
                BooleanQuery.Builder query = new BooleanQuery.Builder();
                for (String token : queryTokens) {
                    query.add(new TermQuery(new Term("f", token)), BooleanClause.Occur.SHOULD);
                }
                Map<Integer, Float> scores = new HashMap<>();
                for (ScoreDoc hit : searcher.search(query.build(), docs.size()).scoreDocs) {
                    int position = searcher.storedFields().document(hit.doc).getField("position").numericValue().intValue();
                    scores.put(position, hit.score);
                }
                return scores;
            }
        }
    }
}
