/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.IntervalIterator;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class SourceIntervalsSourceTests extends ESTestCase {

    private static final IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> SOURCE_FETCHER_PROVIDER =
        context -> docID -> Collections.<Object>singletonList(context.reader().document(docID).get("body"));

    public void testIntervals() throws IOException {
        final FieldType ft = new FieldType(TextField.TYPE_STORED);
        ft.setIndexOptions(IndexOptions.DOCS);
        ft.freeze();
        try (
            Directory dir = newDirectory();
            IndexWriter w = new IndexWriter(
                dir,
                newIndexWriterConfig(Lucene.STANDARD_ANALYZER).setMergePolicy(NoMergePolicy.INSTANCE)
                    .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            )
        ) {

            Document doc = new Document();
            doc.add(new Field("body", "a b", ft));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new Field("body", "b d a d", ft));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new Field("body", "b c d", ft));
            w.addDocument(doc);

            DirectoryReader.open(w).close();

            doc = new Document();
            w.addDocument(doc);

            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals(2, reader.leaves().size());

                IntervalsSource source = new SourceIntervalsSource(
                    Intervals.term(new BytesRef("d")),
                    new TermQuery(new Term("body", "d")),
                    SOURCE_FETCHER_PROVIDER,
                    Lucene.STANDARD_ANALYZER
                );

                IntervalIterator intervals = source.intervals("body", reader.leaves().get(0));

                assertEquals(1, intervals.nextDoc());
                assertEquals(-1, intervals.start());
                assertEquals(-1, intervals.end());
                assertEquals(1, intervals.nextInterval());
                assertEquals(1, intervals.start());
                assertEquals(1, intervals.end());
                assertEquals(3, intervals.nextInterval());
                assertEquals(3, intervals.start());
                assertEquals(3, intervals.end());
                assertEquals(IntervalIterator.NO_MORE_INTERVALS, intervals.nextInterval());

                assertEquals(2, intervals.nextDoc());
                assertEquals(-1, intervals.start());
                assertEquals(-1, intervals.end());
                assertEquals(2, intervals.nextInterval());
                assertEquals(2, intervals.start());
                assertEquals(2, intervals.end());
                assertEquals(IntervalIterator.NO_MORE_INTERVALS, intervals.nextInterval());

                assertEquals(DocIdSetIterator.NO_MORE_DOCS, intervals.nextDoc());

                assertEquals(null, source.intervals("body", reader.leaves().get(1)));

                // Same test, but with a bad approximation now
                source = new SourceIntervalsSource(
                    Intervals.term(new BytesRef("d")),
                    new MatchAllDocsQuery(),
                    SOURCE_FETCHER_PROVIDER,
                    Lucene.STANDARD_ANALYZER
                );

                intervals = source.intervals("body", reader.leaves().get(0));

                assertEquals(1, intervals.nextDoc());
                assertEquals(-1, intervals.start());
                assertEquals(-1, intervals.end());
                assertEquals(1, intervals.nextInterval());
                assertEquals(1, intervals.start());
                assertEquals(1, intervals.end());
                assertEquals(3, intervals.nextInterval());
                assertEquals(3, intervals.start());
                assertEquals(3, intervals.end());
                assertEquals(IntervalIterator.NO_MORE_INTERVALS, intervals.nextInterval());

                assertEquals(2, intervals.nextDoc());
                assertEquals(-1, intervals.start());
                assertEquals(-1, intervals.end());
                assertEquals(2, intervals.nextInterval());
                assertEquals(2, intervals.start());
                assertEquals(2, intervals.end());
                assertEquals(IntervalIterator.NO_MORE_INTERVALS, intervals.nextInterval());

                assertEquals(DocIdSetIterator.NO_MORE_DOCS, intervals.nextDoc());

                intervals = source.intervals("body", reader.leaves().get(1));
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, intervals.nextDoc());
            }
        }
    }
}
