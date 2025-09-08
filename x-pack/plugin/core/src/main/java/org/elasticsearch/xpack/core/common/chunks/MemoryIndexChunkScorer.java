/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.chunks;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for scoring pre-determined chunks using an in-memory Lucene index.
 */
public class MemoryIndexChunkScorer {

    private static final String CONTENT_FIELD = "content";

    private final StandardAnalyzer analyzer;

    public MemoryIndexChunkScorer() {
        // TODO: Allow analyzer to be customizable and/or read from the field mapping
        this.analyzer = new StandardAnalyzer();
    }

    /**
     * Creates an in-memory index of chunks, or chunks, returns ordered, scored list.
     *
     * @param chunks the list of text chunks to score
     * @param inferenceText the query text to compare against
     * @param maxResults maximum number of results to return
     * @return list of scored chunks ordered by relevance
     * @throws IOException on failure scoring chunks
     */
    public List<ScoredChunk> scoreChunks(List<String> chunks, String inferenceText, int maxResults) throws IOException {
        if (chunks == null || chunks.isEmpty() || inferenceText == null || inferenceText.trim().isEmpty()) {
            return new ArrayList<>();
        }

        try (Directory directory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (String chunk : chunks) {
                    Document doc = new Document();
                    doc.add(new TextField(CONTENT_FIELD, chunk, Field.Store.YES));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                org.apache.lucene.util.QueryBuilder qb = new QueryBuilder(analyzer);
                Query query = qb.createBooleanQuery(CONTENT_FIELD, inferenceText, BooleanClause.Occur.SHOULD);
                int numResults = Math.min(maxResults, chunks.size());
                TopDocs topDocs = searcher.search(query, numResults);

                List<ScoredChunk> scoredChunks = new ArrayList<>();
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    Document doc = reader.storedFields().document(scoreDoc.doc);
                    String content = doc.get(CONTENT_FIELD);
                    scoredChunks.add(new ScoredChunk(content, scoreDoc.score));
                }

                // It's possible that no chunks were scorable (for example, a semantic match that does not have a lexical match).
                // In this case, we'll return the first N chunks with a score of 0.
                // TODO: consider parameterizing this
                return scoredChunks.isEmpty() == false
                    ? scoredChunks
                    : chunks.subList(0, Math.min(maxResults, chunks.size())).stream().map(c -> new ScoredChunk(c, 0.0f)).toList();
            }
        }
    }

    /**
     * Represents a chunk with its relevance score.
     */
    public record ScoredChunk(String content, float score) {}
}
