/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

public class FetchPhaseDocsIteratorTests extends ESTestCase {

    public void testInOrderIteration() throws IOException {

        int docCount = random().nextInt(300) + 100;
        Directory directory = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
        for (int i = 0; i < docCount; i++) {
            Document doc = new Document();
            doc.add(new StringField("field", "foo", Field.Store.NO));
            writer.addDocument(doc);
            if (i % 50 == 0) {
                writer.commit();
            }
        }
        writer.commit();
        IndexReader reader = writer.getReader();
        writer.close();

        int[] docs = randomDocIds(docCount - 1);
        FetchPhaseDocsIterator it = new FetchPhaseDocsIterator() {

            LeafReaderContext ctx = null;
            int[] docsInLeaf = null;
            int index = 0;

            @Override
            protected void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) {
                this.ctx = ctx;
                this.docsInLeaf = docsInLeaf;
                for (int i = 0; i < docsInLeaf.length; i++) {
                    if (i > 0) {
                        assertThat(docsInLeaf[i], greaterThan(docsInLeaf[i - 1]));
                    }
                    assertThat(docsInLeaf[i], lessThan(ctx.reader().maxDoc()));
                }
                this.index = 0;
            }

            @Override
            protected SearchHit nextDoc(int doc) {
                assertThat(doc, equalTo(this.docsInLeaf[this.index] + this.ctx.docBase));
                index++;
                return new SearchHit(doc);
            }
        };

        SearchHit[] hits = it.iterate(null, reader, docs, randomBoolean(), new QuerySearchResult());

        assertThat(hits.length, equalTo(docs.length));
        for (int i = 0; i < hits.length; i++) {
            assertThat(hits[i].docId(), equalTo(docs[i]));
            hits[i].decRef();
        }

        reader.close();
        directory.close();

    }

    public void testExceptions() throws IOException {

        int docCount = randomIntBetween(300, 400);
        Directory directory = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
        for (int i = 0; i < docCount; i++) {
            Document doc = new Document();
            doc.add(new StringField("field", "foo", Field.Store.NO));
            writer.addDocument(doc);
            if (i % 50 == 0) {
                writer.commit();
            }
        }
        writer.commit();
        IndexReader reader = writer.getReader();
        writer.close();

        int[] docs = randomDocIds(docCount - 1);
        int badDoc = docs[randomInt(docs.length - 1)];

        FetchPhaseDocsIterator it = new FetchPhaseDocsIterator() {
            @Override
            protected void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) {

            }

            @Override
            protected SearchHit nextDoc(int doc) {
                if (doc == badDoc) {
                    throw new IllegalArgumentException("Error processing doc");
                }
                return new SearchHit(doc);
            }
        };

        Exception e = expectThrows(
            FetchPhaseExecutionException.class,
            () -> it.iterate(null, reader, docs, randomBoolean(), new QuerySearchResult())
        );
        assertThat(e.getMessage(), containsString("Error running fetch phase for doc [" + badDoc + "]"));
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));

        reader.close();
        directory.close();
    }

    private static int[] randomDocIds(int maxDoc) {
        List<Integer> integers = new ArrayList<>();
        int v = 0;
        for (int i = 0; i < 10; i++) {
            v = v + randomInt(maxDoc / 15) + 1;
            if (v >= maxDoc) {
                break;
            }
            integers.add(v);
        }
        Collections.shuffle(integers, random());
        return integers.stream().mapToInt(i -> i).toArray();
    }

}
