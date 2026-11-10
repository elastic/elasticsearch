/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ActivationSortPrefetcherTests extends ESTestCase {

    public void testCreatesSortDocValuesOnEverySegment() throws IOException {
        final int segments = randomIntBetween(2, 6);
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int s = 0; s < segments; s++) {
                    for (int d = 0; d < 10; d++) {
                        Document doc = new Document();
                        doc.add(new SortedNumericDocValuesField("ts", s * 10L + d));
                        doc.add(new LongPoint("ts", s * 10L + d));
                        if (s > 0) { // the first segment has no kw field
                            doc.add(new SortedSetDocValuesField("kw", new BytesRef("v" + d)));
                        }
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.leaves().size(), equalTo(segments));
                final SortField[] sort = new SortField[] {
                    new SortedNumericSortField("ts", SortField.Type.LONG, true),
                    new SortField("kw", SortField.Type.STRING),
                    SortField.FIELD_SCORE,
                    SortField.FIELD_DOC,
                    new SortField("missing", SortField.Type.LONG) };
                assertThat(ActivationSortPrefetcher.prefetch(reader.leaves(), sort), equalTo(segments + segments - 1));
            }
        }
    }
}
