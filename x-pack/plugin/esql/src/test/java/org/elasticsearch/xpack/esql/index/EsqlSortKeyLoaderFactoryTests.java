/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;

/** The key loader factory resolves ordinals to the same values as the doc values. */
public class EsqlSortKeyLoaderFactoryTests extends ESTestCase {

    public void testSortedSetOrdinals() throws IOException {
        int numDocs = between(2, 2000);
        List<TreeSet<String>> expected = new ArrayList<>();
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                TreeSet<String> values = new TreeSet<>();
                if (i == 0) {
                    // The smallest value, only in a document that the block skips
                    values.add("0");
                } else if (i == 1) {
                    // At least one document has several values
                    values.add("a");
                    values.add("b");
                }
                int count = i == 0 ? 0 : between(0, 4);
                for (int v = 0; v < count; v++) {
                    values.add(randomAlphaOfLength(between(1, 8)));
                }
                Document doc = new Document();
                for (String value : values) {
                    doc.add(new SortedSetDocValuesField("f", new BytesRef(value)));
                }
                writer.addDocument(doc);
                expected.add(values);
            }
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().getFirst().reader();
                SortedSetDocValues dv = leaf.getSortedSetDocValues("f");
                List<Integer> docs = subset(numDocs);
                try (BlockLoader.SortedSetOrdinalsBuilder builder = EsqlSortKey.LOADER_FACTORY.sortedSetOrdinalsBuilder(dv, docs.size())) {
                    for (int doc : docs) {
                        if (dv.advanceExact(doc) == false) {
                            builder.appendNull();
                            continue;
                        }
                        int count = dv.docValueCount();
                        if (count > 1) {
                            builder.beginPositionEntry();
                        }
                        for (int v = 0; v < count; v++) {
                            builder.appendOrd(Math.toIntExact(dv.nextOrd()));
                        }
                        if (count > 1) {
                            builder.endPositionEntry();
                        }
                    }
                    try (BytesRefBlock block = (BytesRefBlock) builder.build()) {
                        assertThat(block.getPositionCount(), equalTo(docs.size()));
                        BytesRef scratch = new BytesRef();
                        for (int p = 0; p < docs.size(); p++) {
                            List<String> actual = new ArrayList<>();
                            int first = block.getFirstValueIndex(p);
                            for (int v = 0; v < block.getValueCount(p); v++) {
                                actual.add(block.getBytesRef(first + v, scratch).utf8ToString());
                            }
                            assertThat(actual, equalTo(new ArrayList<>(expected.get(docs.get(p)))));
                        }
                    }
                }
            }
        }
    }

    public void testSingletonOrdinals() throws IOException {
        int numDocs = between(2, 2000);
        String[] expected = new String[numDocs];
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                if (i == 0) {
                    // The smallest value, only in a document that the block skips
                    expected[i] = "0";
                    doc.add(new SortedDocValuesField("f", new BytesRef(expected[i])));
                } else if (i == 1 || randomBoolean()) {
                    expected[i] = randomAlphaOfLength(between(1, 8));
                    doc.add(new SortedDocValuesField("f", new BytesRef(expected[i])));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().getFirst().reader();
                SortedDocValues dv = leaf.getSortedDocValues("f");
                List<Integer> docs = subset(numDocs);
                try (
                    BlockLoader.SingletonOrdinalsBuilder builder = EsqlSortKey.LOADER_FACTORY.singletonOrdinalsBuilder(
                        dv,
                        docs.size(),
                        false
                    )
                ) {
                    for (int doc : docs) {
                        if (dv.advanceExact(doc)) {
                            builder.appendOrd(dv.ordValue());
                        } else {
                            builder.appendNull();
                        }
                    }
                    try (BytesRefBlock block = (BytesRefBlock) builder.build()) {
                        assertThat(block.getPositionCount(), equalTo(docs.size()));
                        BytesRef scratch = new BytesRef();
                        for (int p = 0; p < docs.size(); p++) {
                            String value = expected[docs.get(p)];
                            if (value == null) {
                                assertTrue(block.isNull(p));
                            } else {
                                assertThat(block.getBytesRef(block.getFirstValueIndex(p), scratch).utf8ToString(), equalTo(value));
                            }
                        }
                    }
                }
            }
        }
    }

    /** A random subset of the documents that skips the first one and keeps the second, so the block misses some ordinals. */
    private static List<Integer> subset(int numDocs) {
        List<Integer> docs = new ArrayList<>();
        for (int doc = 1; doc < numDocs; doc++) {
            if (doc == 1 || randomBoolean()) {
                docs.add(doc);
            }
        }
        return docs;
    }
}
