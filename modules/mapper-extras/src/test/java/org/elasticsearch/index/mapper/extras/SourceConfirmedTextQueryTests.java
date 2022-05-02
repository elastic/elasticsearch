/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.CheckHits;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class SourceConfirmedTextQueryTests extends ESTestCase {

    private static final Function<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> SOURCE_FETCHER_PROVIDER = context -> {
        return docID -> Collections.<Object>singletonList(context.reader().document(docID).get("body"));
    };

    public void testTerm() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(Lucene.STANDARD_ANALYZER))) {

            Document doc = new Document();
            doc.add(new TextField("body", "a b c b a b c", Store.YES));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new TextField("body", "b d", Store.YES));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new TextField("body", "b c d", Store.YES));
            w.addDocument(doc);

            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                TermQuery query = new TermQuery(new Term("body", "c"));
                Query sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);

                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                ScoreDoc[] phraseHits = searcher.search(query, 10).scoreDocs;
                assertEquals(2, phraseHits.length);
                ScoreDoc[] sourceConfirmedHits = searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs;
                CheckHits.checkEqual(query, phraseHits, sourceConfirmedHits);
                CheckHits.checkExplanations(sourceConfirmedPhraseQuery, "body", searcher);

                // Term query with missing term
                query = new TermQuery(new Term("body", "e"));
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                assertArrayEquals(new ScoreDoc[0], searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs);
            }
        }
    }

    public void testPhrase() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(Lucene.STANDARD_ANALYZER))) {

            Document doc = new Document();
            doc.add(new TextField("body", "a b c b a b c", Store.YES));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new TextField("body", "b d", Store.YES));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new TextField("body", "b c d", Store.YES));
            w.addDocument(doc);

            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                PhraseQuery query = new PhraseQuery("body", "b", "c");
                Query sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);

                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                ScoreDoc[] phraseHits = searcher.search(query, 10).scoreDocs;
                assertEquals(2, phraseHits.length);
                ScoreDoc[] sourceConfirmedHits = searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs;
                CheckHits.checkEqual(query, phraseHits, sourceConfirmedHits);
                CheckHits.checkExplanations(sourceConfirmedPhraseQuery, "body", searcher);

                // Sloppy phrase query
                query = new PhraseQuery(1, "body", "b", "d");
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                phraseHits = searcher.search(query, 10).scoreDocs;
                assertEquals(2, phraseHits.length);
                sourceConfirmedHits = searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs;
                CheckHits.checkEqual(query, phraseHits, sourceConfirmedHits);
                CheckHits.checkExplanations(sourceConfirmedPhraseQuery, "body", searcher);

                // Phrase query with no matches
                query = new PhraseQuery("body", "d", "c");
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                assertArrayEquals(new ScoreDoc[0], searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs);

                // Phrase query with one missing term
                query = new PhraseQuery("body", "b", "e");
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                assertArrayEquals(new ScoreDoc[0], searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs);
            }
        }
    }

    public void testMultiPhrase() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(Lucene.STANDARD_ANALYZER))) {

            Document doc = new Document();
            doc.add(new TextField("body", "a b c b a b c", Store.YES));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new TextField("body", "b d", Store.YES));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new TextField("body", "b c d", Store.YES));
            w.addDocument(doc);

            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                MultiPhraseQuery query = new MultiPhraseQuery.Builder().add(new Term[] { new Term("body", "a"), new Term("body", "b") }, 0)
                    .add(new Term[] { new Term("body", "c") }, 1)
                    .build();

                Query sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);

                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));

                ScoreDoc[] phraseHits = searcher.search(query, 10).scoreDocs;
                assertEquals(2, phraseHits.length);
                ScoreDoc[] sourceConfirmedHits = searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs;
                CheckHits.checkEqual(query, phraseHits, sourceConfirmedHits);
                CheckHits.checkExplanations(sourceConfirmedPhraseQuery, "body", searcher);

                // Sloppy multi phrase query
                query = new MultiPhraseQuery.Builder().add(new Term[] { new Term("body", "a"), new Term("body", "b") }, 0)
                    .add(new Term[] { new Term("body", "d") }, 1)
                    .setSlop(1)
                    .build();
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                phraseHits = searcher.search(query, 10).scoreDocs;
                assertEquals(2, phraseHits.length);
                sourceConfirmedHits = searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs;
                CheckHits.checkEqual(query, phraseHits, sourceConfirmedHits);
                CheckHits.checkExplanations(sourceConfirmedPhraseQuery, "body", searcher);

                // Multi phrase query with no matches
                query = new MultiPhraseQuery.Builder().add(new Term[] { new Term("body", "d"), new Term("body", "c") }, 0)
                    .add(new Term[] { new Term("body", "a") }, 1)
                    .build();
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                assertArrayEquals(new ScoreDoc[0], searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs);

                // Multi phrase query with one missing term
                query = new MultiPhraseQuery.Builder().add(new Term[] { new Term("body", "d"), new Term("body", "c") }, 0)
                    .add(new Term[] { new Term("body", "e") }, 1)
                    .build();
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                assertArrayEquals(new ScoreDoc[0], searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs);
            }
        }
    }

    public void testMultiPhrasePrefix() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(Lucene.STANDARD_ANALYZER))) {

            Document doc = new Document();
            doc.add(new TextField("body", "a b cd b a b cd", Store.YES));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new TextField("body", "b d", Store.YES));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new TextField("body", "b cd e", Store.YES));
            w.addDocument(doc);

            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                MultiPhrasePrefixQuery query = new MultiPhrasePrefixQuery("body");
                Query sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                ScoreDoc[] phrasePrefixHits = searcher.search(query, 10).scoreDocs;
                ScoreDoc[] sourceConfirmedHits = searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs;
                CheckHits.checkEqual(query, phrasePrefixHits, sourceConfirmedHits);
                CheckHits.checkExplanations(sourceConfirmedPhraseQuery, "body", searcher);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));

                query = new MultiPhrasePrefixQuery("body");
                query.add(new Term("body", "c"));
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                phrasePrefixHits = searcher.search(query, 10).scoreDocs;
                sourceConfirmedHits = searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs;
                CheckHits.checkEqual(query, phrasePrefixHits, sourceConfirmedHits);
                CheckHits.checkExplanations(sourceConfirmedPhraseQuery, "body", searcher);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));

                query = new MultiPhrasePrefixQuery("body");
                query.add(new Term("body", "b"));
                query.add(new Term("body", "c"));
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                phrasePrefixHits = searcher.search(query, 10).scoreDocs;
                sourceConfirmedHits = searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs;
                CheckHits.checkEqual(query, phrasePrefixHits, sourceConfirmedHits);
                CheckHits.checkExplanations(sourceConfirmedPhraseQuery, "body", searcher);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));

                // Sloppy multi phrase prefix query
                query = new MultiPhrasePrefixQuery("body");
                query.add(new Term("body", "a"));
                query.add(new Term("body", "c"));
                query.setSlop(2);
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                phrasePrefixHits = searcher.search(query, 10).scoreDocs;
                sourceConfirmedHits = searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs;
                CheckHits.checkEqual(query, phrasePrefixHits, sourceConfirmedHits);
                CheckHits.checkExplanations(sourceConfirmedPhraseQuery, "body", searcher);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));

                // Multi phrase prefix query with no matches
                query = new MultiPhrasePrefixQuery("body");
                query.add(new Term("body", "d"));
                query.add(new Term("body", "b"));
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                assertArrayEquals(new ScoreDoc[0], searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs);

                // Multi phrase query with one missing term
                query = new MultiPhrasePrefixQuery("body");
                query.add(new Term("body", "d"));
                query.add(new Term("body", "f"));
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                assertEquals(0, searcher.count(sourceConfirmedPhraseQuery));
                assertArrayEquals(new ScoreDoc[0], searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs);
            }
        }
    }

    public void testSpanNear() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(Lucene.STANDARD_ANALYZER))) {

            Document doc = new Document();
            doc.add(new TextField("body", "a b c b a b c", Store.YES));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new TextField("body", "b d", Store.YES));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new TextField("body", "b c d", Store.YES));
            w.addDocument(doc);

            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                SpanNearQuery query = new SpanNearQuery(
                    new SpanQuery[] { new SpanTermQuery(new Term("body", "b")), new SpanTermQuery(new Term("body", "c")) },
                    0,
                    false
                );
                Query sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);

                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                ScoreDoc[] spanHits = searcher.search(query, 10).scoreDocs;
                assertEquals(2, spanHits.length);
                ScoreDoc[] sourceConfirmedHits = searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs;
                CheckHits.checkEqual(query, spanHits, sourceConfirmedHits);
                CheckHits.checkExplanations(sourceConfirmedPhraseQuery, "body", searcher);

                // Sloppy span near query
                query = new SpanNearQuery(
                    new SpanQuery[] { new SpanTermQuery(new Term("body", "b")), new SpanTermQuery(new Term("body", "c")) },
                    1,
                    false
                );
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                spanHits = searcher.search(query, 10).scoreDocs;
                assertEquals(2, spanHits.length);
                sourceConfirmedHits = searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs;
                CheckHits.checkEqual(query, spanHits, sourceConfirmedHits);
                CheckHits.checkExplanations(sourceConfirmedPhraseQuery, "body", searcher);

                // Span near query with no matches
                query = new SpanNearQuery(
                    new SpanQuery[] { new SpanTermQuery(new Term("body", "a")), new SpanTermQuery(new Term("body", "d")) },
                    0,
                    false
                );
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                assertArrayEquals(new ScoreDoc[0], searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs);

                // Span near query with one missing term
                query = new SpanNearQuery(
                    new SpanQuery[] { new SpanTermQuery(new Term("body", "b")), new SpanTermQuery(new Term("body", "e")) },
                    0,
                    false
                );
                sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                assertEquals(searcher.count(query), searcher.count(sourceConfirmedPhraseQuery));
                assertArrayEquals(new ScoreDoc[0], searcher.search(sourceConfirmedPhraseQuery, 10).scoreDocs);
            }
        }
    }

    public void testToString() {
        PhraseQuery query = new PhraseQuery("body", "b", "c");
        Query sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
        assertEquals(query.toString(), sourceConfirmedPhraseQuery.toString());
    }

    public void testEqualsHashCode() {
        PhraseQuery query1 = new PhraseQuery("body", "b", "c");
        Query sourceConfirmedPhraseQuery1 = new SourceConfirmedTextQuery(query1, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);

        assertEquals(sourceConfirmedPhraseQuery1, sourceConfirmedPhraseQuery1);
        assertEquals(sourceConfirmedPhraseQuery1.hashCode(), sourceConfirmedPhraseQuery1.hashCode());

        PhraseQuery query2 = new PhraseQuery("body", "b", "c");
        Query sourceConfirmedPhraseQuery2 = new SourceConfirmedTextQuery(query2, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
        assertEquals(sourceConfirmedPhraseQuery1, sourceConfirmedPhraseQuery2);

        PhraseQuery query3 = new PhraseQuery("body", "b", "d");
        Query sourceConfirmedPhraseQuery3 = new SourceConfirmedTextQuery(query3, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
        assertNotEquals(sourceConfirmedPhraseQuery1, sourceConfirmedPhraseQuery3);

        Query sourceConfirmedPhraseQuery4 = new SourceConfirmedTextQuery(query1, context -> null, Lucene.STANDARD_ANALYZER);
        assertNotEquals(sourceConfirmedPhraseQuery1, sourceConfirmedPhraseQuery4);

        Query sourceConfirmedPhraseQuery5 = new SourceConfirmedTextQuery(query1, SOURCE_FETCHER_PROVIDER, Lucene.KEYWORD_ANALYZER);
        assertNotEquals(sourceConfirmedPhraseQuery1, sourceConfirmedPhraseQuery5);
    }

    public void testApproximation() {
        assertEquals(
            new TermQuery(new Term("body", "text")),
            SourceConfirmedTextQuery.approximate(new TermQuery(new Term("body", "text")))
        );

        assertEquals(
            new BooleanQuery.Builder().add(new TermQuery(new Term("body", "a")), Occur.FILTER)
                .add(new TermQuery(new Term("body", "b")), Occur.FILTER)
                .build(),
            SourceConfirmedTextQuery.approximate(new PhraseQuery("body", "a", "b"))
        );

        MultiPhraseQuery query = new MultiPhraseQuery.Builder().add(new Term("body", "a"))
            .add(new Term[] { new Term("body", "b"), new Term("body", "c") })
            .build();
        Query approximation = new BooleanQuery.Builder().add(
            new BooleanQuery.Builder().add(new TermQuery(new Term("body", "a")), Occur.SHOULD).build(),
            Occur.FILTER
        )
            .add(
                new BooleanQuery.Builder().add(new TermQuery(new Term("body", "b")), Occur.SHOULD)
                    .add(new TermQuery(new Term("body", "c")), Occur.SHOULD)
                    .build(),
                Occur.FILTER
            )
            .build();
        assertEquals(approximation, SourceConfirmedTextQuery.approximate(query));

        MultiPhrasePrefixQuery phrasePrefixQuery = new MultiPhrasePrefixQuery("body");
        assertEquals(new MatchNoDocsQuery(), SourceConfirmedTextQuery.approximate(phrasePrefixQuery));

        phrasePrefixQuery.add(new Term("body", "apache"));
        approximation = new BooleanQuery.Builder().add(new PrefixQuery(new Term("body", "apache")), Occur.FILTER).build();
        assertEquals(approximation, SourceConfirmedTextQuery.approximate(phrasePrefixQuery));

        phrasePrefixQuery.add(new Term("body", "luc"));
        approximation = new BooleanQuery.Builder().add(
            new BooleanQuery.Builder().add(new TermQuery(new Term("body", "apache")), Occur.SHOULD).build(),
            Occur.FILTER
        ).build();
        assertEquals(approximation, SourceConfirmedTextQuery.approximate(phrasePrefixQuery));
    }

    public void testEmptyIndex() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(Lucene.STANDARD_ANALYZER))) {
            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                PhraseQuery query = new PhraseQuery("body", "a", "b");
                Query sourceConfirmedPhraseQuery = new SourceConfirmedTextQuery(query, SOURCE_FETCHER_PROVIDER, Lucene.STANDARD_ANALYZER);
                assertEquals(0, searcher.count(sourceConfirmedPhraseQuery));
            }
        }
    }

}
