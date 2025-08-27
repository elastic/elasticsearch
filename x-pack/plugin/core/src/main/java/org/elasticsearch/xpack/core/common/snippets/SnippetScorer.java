/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.snippets;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for scoring snippets using an in-memory Lucene index.
 */
public class SnippetScorer {

    private static final String CONTENT_FIELD = "content";

    private final StandardAnalyzer analyzer;

    public SnippetScorer() {
        this.analyzer = new StandardAnalyzer();
    }

    /**
     * Creates an in-memory index of snippets, or snippets, returns ordered, scored list.
     *
     * @param snippets the list of text snippets to score
     * @param inferenceText the query text to compare against
     * @param maxResults maximum number of results to return
     * @return list of scored snippets ordered by relevance
     * @throws IOException on failure scoring snippets
     */
    public List<ScoredSnippet> scoreSnippets(List<String> snippets, String inferenceText, int maxResults) throws IOException {
        if (snippets == null || snippets.isEmpty() || inferenceText == null || inferenceText.trim().isEmpty()) {
            return new ArrayList<>();
        }

        try (Directory directory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (String snippet : snippets) {
                    Document doc = new Document();
                    doc.add(new TextField(CONTENT_FIELD, snippet, Field.Store.YES));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                Query query = createQuery(inferenceText);
                int numResults = Math.min(maxResults, snippets.size());
                TopDocs topDocs = searcher.search(query, numResults);

                List<ScoredSnippet> scoredSnippets = new ArrayList<>();
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    Document doc = reader.storedFields().document(scoreDoc.doc);
                    String content = doc.get(CONTENT_FIELD);
                    scoredSnippets.add(new ScoredSnippet(content, scoreDoc.score));
                }

                return scoredSnippets;
            }
        }
    }

    /**
     * Creates a Lucene query from the inference text.
     */
    private Query createQuery(String inferenceText) throws IOException {
        String[] tokens = tokenizeText(inferenceText);

        if (tokens.length == 0) {
            throw new IllegalArgumentException("Inference text must contain at least one valid token");
        } else if (tokens.length == 1) {
            return new TermQuery(new Term(CONTENT_FIELD, tokens[0]));
        } else {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (String token : tokens) {
                if (token != null && token.trim().isEmpty() == false) {
                    builder.add(new TermQuery(new Term(CONTENT_FIELD, token)), BooleanClause.Occur.SHOULD);
                }
            }
            return builder.build();
        }
    }

    private String[] tokenizeText(String text) throws IOException {
        List<String> tokens = new ArrayList<>();
        try (org.apache.lucene.analysis.TokenStream tokenStream = analyzer.tokenStream(CONTENT_FIELD, text)) {
            org.apache.lucene.analysis.tokenattributes.CharTermAttribute termAttribute = tokenStream.addAttribute(
                org.apache.lucene.analysis.tokenattributes.CharTermAttribute.class
            );
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                tokens.add(termAttribute.toString());
            }
            tokenStream.end();
        }
        return tokens.toArray(new String[0]);
    }

    /**
     * Represents a snippet with its relevance score.
     */
    public record ScoredSnippet(String content, float score) {}
}
