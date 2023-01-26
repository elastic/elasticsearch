/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LinearRankFeatureTermQueryTests extends ESTestCase {

    public void testScoring() throws IOException {
        String fieldName = "sparse_terms";
        List<String> terms = List.of("1", "2", "3", "4");
        List<Float> scores = List.of(1f, 2f, 5f, 9f);
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())))) {
                for (int i = 0; i < terms.size() + 1; i++) {
                    Document d = new Document();
                    d.add(new TextField("id", Integer.toString(i), Field.Store.YES));
                    for (int j = 1; j <= i; j++) {
                        d.add(new FeatureField(fieldName, terms.get(j - 1), scores.get(j - 1)));
                    }
                    w.addDocument(d);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);

                TopDocs topDocs = searcher.search(new LinearRankFeatureTermQuery(new Term(fieldName, "1")), 10);
                assertThat(topDocs.totalHits.value, equalTo((long) terms.size()));
                for (ScoreDoc d : topDocs.scoreDocs) {
                    assertThat(d.score, equalTo(1f));
                }

                topDocs = searcher.search(new BoostQuery(new LinearRankFeatureTermQuery(new Term(fieldName, "1")), 5f), 10);
                assertThat(topDocs.totalHits.value, equalTo((long) terms.size()));
                for (ScoreDoc d : topDocs.scoreDocs) {
                    assertThat(d.score, equalTo(5f));
                }

                topDocs = searcher.search(new LinearRankFeatureTermQuery(new Term(fieldName, "2")), 10);
                assertThat(topDocs.totalHits.value, equalTo((long) terms.size() - 1));
                for (ScoreDoc d : topDocs.scoreDocs) {
                    assertThat(d.score, equalTo(2f));
                }

                topDocs = searcher.search(
                    new BooleanQuery.Builder().setMinimumNumberShouldMatch(1)
                        .add(new LinearRankFeatureTermQuery(new Term(fieldName, "1")), BooleanClause.Occur.SHOULD)
                        .add(new LinearRankFeatureTermQuery(new Term(fieldName, "2")), BooleanClause.Occur.SHOULD)
                        .build(),
                    10
                );
                assertThat(topDocs.totalHits.value, equalTo((long) terms.size()));
                for (ScoreDoc d : topDocs.scoreDocs) {
                    if (searcher.storedFields().document(d.doc).getField("id").stringValue().equals("1")) {
                        assertThat(d.score, equalTo(1f));
                    } else {
                        assertThat(d.score, equalTo(3f));
                    }
                }
            }
        }
    }

    public void testConstantScoring() throws IOException {
        String fieldName = "sparse_terms";
        List<String> terms = List.of("1", "2", "3", "4");
        List<Float> scores = List.of(1f, 2f, 5f, 9f);
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())))) {
                for (int i = 0; i < terms.size() + 1; i++) {
                    Document d = new Document();
                    d.add(new TextField("id", Integer.toString(i), Field.Store.YES));
                    for (int j = 1; j <= i; j++) {
                        d.add(new FeatureField(fieldName, terms.get(j - 1), scores.get(j - 1)));
                    }
                    w.addDocument(d);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                TopDocs topDocs = searcher.search(new ConstantScoreQuery(new LinearRankFeatureTermQuery(new Term(fieldName, "1"))), 10);
                assertThat(topDocs.totalHits.value, equalTo((long) terms.size()));
                for (ScoreDoc d : topDocs.scoreDocs) {
                    assertThat(d.score, equalTo(1f));
                }
            }
        }
    }
}
