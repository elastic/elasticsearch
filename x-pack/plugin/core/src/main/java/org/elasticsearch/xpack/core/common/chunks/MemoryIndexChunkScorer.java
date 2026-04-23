/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.chunks;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for scoring pre-determined chunks using an in-memory Lucene index.
 */
public class MemoryIndexChunkScorer {

    public static final String CONTENT_FIELD = "content";

    private final Analyzer analyzer;

    public MemoryIndexChunkScorer() {
        // TODO: Allow analyzer to be customizable and/or read from the field mapping
        this(new StandardAnalyzer());
    }

    /**
     * @param analyzer the analyzer used for indexing chunks and parsing query text.
     */
    public MemoryIndexChunkScorer(Analyzer analyzer) {
        assert analyzer != null : "Analyzer should be set";
        this.analyzer = analyzer;
    }

    public Analyzer analyzer() {
        return analyzer;
    }

    /**
     * Parses {@code queryText} into a boolean-OR query over {@link #CONTENT_FIELD}
     * using this scorer's analyzer. Returns {@code null} when no terms survive
     * analysis (e.g. all stop-words).
     */
    public Query buildQuery(String queryText) {
        return new QueryBuilder(analyzer).createBooleanQuery(CONTENT_FIELD, queryText, BooleanClause.Occur.SHOULD);
    }

    /**
     * Creates an in-memory index of chunks, scores them against the query text, and
     * returns the top results ordered by relevance.
     *
     * @param chunks the list of text chunks to score
     * @param queryText the query text to compare against
     * @param maxResults maximum number of results to return
     * @param backfillResults if true, backfills no matches with the first chunks in the list with scores of 0
     * @return list of scored chunks ordered by relevance
     * @throws ElasticsearchException on failure scoring chunks
     */
    public List<ScoredChunk> scoreChunks(List<String> chunks, String queryText, int maxResults, boolean backfillResults) {
        if (chunks == null || chunks.isEmpty() || queryText == null || Strings.isNullOrBlank(queryText)) {
            return new ArrayList<>();
        }
        Query query = buildQuery(queryText);
        return scoreChunks(chunks, query, maxResults, backfillResults);
    }

    /**
     * Scores chunks against a pre-built {@link Query}.
     *
     * @param chunks the list of text chunks to score
     * @param query the Lucene query to score against; if {@code null} no chunks will match
     * @param maxResults maximum number of results to return
     * @param backfillResults if true, backfills no matches with the first chunks in the list with scores of 0
     * @return list of scored chunks ordered by relevance
     * @throws ElasticsearchException on failure scoring chunks
     */
    public List<ScoredChunk> scoreChunks(List<String> chunks, Query query, int maxResults, boolean backfillResults) {
        if (chunks == null || chunks.isEmpty()) {
            return new ArrayList<>();
        }

        if (query == null) {
            return backfillResults
                ? chunks.subList(0, Math.min(maxResults, chunks.size())).stream().map(c -> new ScoredChunk(c, 0.0f)).toList()
                : new ArrayList<>();
        }

        try (Directory directory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (String chunk : chunks) {
                    Document doc = new Document();
                    doc.add(new TextField(CONTENT_FIELD, chunk, Store.NO));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                int numResults = Math.min(maxResults, chunks.size());
                TopDocs topDocs = searcher.search(query, numResults);

                List<ScoredChunk> scoredChunks = new ArrayList<>();
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    scoredChunks.add(new ScoredChunk(chunks.get(scoreDoc.doc), scoreDoc.score));
                }

                return backfillResults && scoredChunks.isEmpty()
                    ? chunks.subList(0, Math.min(maxResults, chunks.size())).stream().map(c -> new ScoredChunk(c, 0.0f)).toList()
                    : scoredChunks;
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to score chunks", e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
