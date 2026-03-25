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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Utility class for scoring pre-determined chunks using an in-memory Lucene index.
 * <p>
 * For one-shot scoring, use {@link #scoreChunks}. When both scoring and additional
 * operations (e.g. highlighting) are needed on the same chunks, use {@link #openSession}
 * to build the index once and reuse it.
 */
public class MemoryIndexChunkScorer {

    public static final String CONTENT_FIELD = "content";

    /**
     * Lightweight field for scoring only — no stored value, no positions or offsets.
     */
    private static final FieldType SCORING_FIELD_TYPE = TextField.TYPE_NOT_STORED;

    /**
     * Field with positions and offsets in the postings so unified highlighting can use
     * {@code OffsetSource.POSTINGS} instead of re-analyzing the chunk. Not stored because
     * callers already hold the original chunk list and can look up content by Lucene doc id.
     */
    private static final FieldType HIGHLIGHTING_FIELD_TYPE = highlightingFieldType();

    private static FieldType highlightingFieldType() {
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        ft.freeze();
        return ft;
    }

    private final Analyzer analyzer;

    /**
     * Builds a scorer that indexes and queries chunks with Lucene's {@link StandardAnalyzer}.
     */
    public MemoryIndexChunkScorer() {
        this(new StandardAnalyzer());
    }

    /**
     * Builds a scorer that indexes and queries chunks with the given analyzer. Callers that need
     * snippet scoring and highlighting to align with a mapped field should pass the same
     * {@link Analyzer} used for that field at index/search time when it is available.
     *
     * @param analyzer non-null; used for {@link org.apache.lucene.index.IndexWriterConfig} and query parsing
     */
    public MemoryIndexChunkScorer(Analyzer analyzer) {
        this.analyzer = Objects.requireNonNull(analyzer, "analyzer");
    }

    /**
     * Creates an in-memory index of chunks, or chunks, returns ordered, scored list.
     *
     * @param chunks the list of text chunks to score
     * @param inferenceText the query text to compare against
     * @param maxResults maximum number of results to return
     * @param backfillResults If true, backfills no matches with the first chunks in the list with scores of 0.
     * @return list of scored chunks ordered by relevance
     * @throws ElasticsearchException on failure scoring chunks
     */
    public List<ScoredChunk> scoreChunks(List<String> chunks, String inferenceText, int maxResults, boolean backfillResults) {
        if (chunks == null || chunks.isEmpty() || inferenceText == null || inferenceText.trim().isEmpty()) {
            return new ArrayList<>();
        }
        try (Session session = openSession(chunks, false)) {
            return session.score(inferenceText, maxResults, backfillResults);
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to score chunks", e);
        }
    }

    /**
     * Opens a reusable session that indexes the given chunks once. The caller can then
     * {@link Session#score score} against the in-memory index, and access the underlying
     * Lucene components (via {@link Session#searcher()}, {@link Session#reader()},
     * {@link Session#analyzer()}) for additional operations such as highlighting.
     *
     * @param chunks the text chunks to index
     * @param withOffsets if {@code true}, indexes positions and offsets for postings-based highlighting;
     *                    if {@code false}, uses a lighter index suitable for scoring only
     * @return a closeable session; the caller is responsible for closing it
     */
    public Session openSession(List<String> chunks, boolean withOffsets) {
        return new Session(chunks, analyzer, withOffsets ? HIGHLIGHTING_FIELD_TYPE : SCORING_FIELD_TYPE);
    }

    public Session openSession(List<String> chunks) {
        return new Session(chunks, analyzer, SCORING_FIELD_TYPE);
    }

    /**
     * Keeps an in-memory Lucene index alive across multiple operations so the index is built
     * once and reused. Exposes the underlying {@link IndexSearcher}, {@link DirectoryReader},
     * and {@link Analyzer} for consumers that need direct access (e.g. highlighting).
     * Implements {@link Closeable} to manage the directory and reader lifecycle.
     */
    public static class Session implements Closeable {

        private final Directory directory;
        private final DirectoryReader reader;
        private final IndexSearcher searcher;
        private final Analyzer analyzer;
        private final List<String> chunks;

        Session(List<String> chunks, Analyzer analyzer, FieldType fieldType) {
            this.analyzer = analyzer;
            this.chunks = chunks;
            boolean success = false;
            Directory dir = new ByteBuffersDirectory();
            DirectoryReader dirReader = null;
            try {
                IndexWriterConfig config = new IndexWriterConfig(analyzer);
                try (IndexWriter writer = new IndexWriter(dir, config)) {
                    for (String chunk : chunks) {
                        Document doc = new Document();
                        doc.add(new Field(CONTENT_FIELD, chunk, fieldType));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
                dirReader = DirectoryReader.open(dir);
                this.directory = dir;
                this.reader = dirReader;
                this.searcher = new IndexSearcher(reader);
                success = true;
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to build in-memory chunk index", e);
            } finally {
                if (success == false) {
                    try {
                        if (dirReader != null) {
                            dirReader.close();
                        }
                    } catch (IOException ignored) {} finally {
                        try {
                            dir.close();
                        } catch (IOException ignored) {}
                    }
                }
            }
        }

        /**
         * Scores indexed chunks against the given query text using BM25.
         *
         * @param queryText the query text to compare against
         * @param maxResults maximum number of results to return
         * @param backfillResults if true, backfills no matches with the first chunks with scores of 0
         * @return scored chunks ordered by relevance
         */
        public List<ScoredChunk> score(String queryText, int maxResults, boolean backfillResults) {
            try {
                QueryBuilder qb = new QueryBuilder(analyzer);
                Query query = qb.createBooleanQuery(CONTENT_FIELD, queryText, BooleanClause.Occur.SHOULD);
                int numResults = Math.min(maxResults, chunks.size());
                TopDocs topDocs = searcher.search(query, numResults);

                List<ScoredChunk> scoredChunks = new ArrayList<>();
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    scoredChunks.add(new ScoredChunk(chunks.get(scoreDoc.doc), scoreDoc.score, scoreDoc.doc));
                }

                return backfillResults && scoredChunks.isEmpty()
                    ? IntStream.range(0, Math.min(maxResults, chunks.size()))
                        .mapToObj(i -> new ScoredChunk(chunks.get(i), 0.0f, i))
                        .toList()
                    : scoredChunks;
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to score chunks", e);
            }
        }

        public IndexSearcher searcher() {
            return searcher;
        }

        public DirectoryReader reader() {
            return reader;
        }

        public Analyzer analyzer() {
            return analyzer;
        }

        @Override
        public void close() throws IOException {
            reader.close();
            directory.close();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
