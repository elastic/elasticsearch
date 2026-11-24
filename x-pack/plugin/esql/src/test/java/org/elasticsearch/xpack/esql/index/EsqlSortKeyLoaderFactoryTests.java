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
        int numDocs = between(1, 2000);
        List<TreeSet<String>> expected = new ArrayList<>();
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                TreeSet<String> values = new TreeSet<>();
                int count = between(0, 4);
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
                try (BlockLoader.SortedSetOrdinalsBuilder builder = EsqlSortKey.LOADER_FACTORY.sortedSetOrdinalsBuilder(dv, numDocs)) {
                    for (int doc = 0; doc < numDocs; doc++) {
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
                        assertThat(block.getPositionCount(), equalTo(numDocs));
                        BytesRef scratch = new BytesRef();
                        for (int doc = 0; doc < numDocs; doc++) {
                            List<String> actual = new ArrayList<>();
                            int first = block.getFirstValueIndex(doc);
                            for (int v = 0; v < block.getValueCount(doc); v++) {
                                actual.add(block.getBytesRef(first + v, scratch).utf8ToString());
                            }
                            assertThat(actual, equalTo(new ArrayList<>(expected.get(doc))));
                        }
                    }
                }
            }
        }
    }

    public void testSingletonOrdinals() throws IOException {
        int numDocs = between(1, 2000);
        String[] expected = new String[numDocs];
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                if (randomBoolean()) {
                    expected[i] = randomAlphaOfLength(between(1, 8));
                    doc.add(new SortedDocValuesField("f", new BytesRef(expected[i])));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().getFirst().reader();
                SortedDocValues dv = leaf.getSortedDocValues("f");
                if (dv == null) {
                    return;
                }
                try (
                    BlockLoader.SingletonOrdinalsBuilder builder = EsqlSortKey.LOADER_FACTORY.singletonOrdinalsBuilder(dv, numDocs, false)
                ) {
                    for (int doc = 0; doc < numDocs; doc++) {
                        if (dv.advanceExact(doc)) {
                            builder.appendOrd(dv.ordValue());
                        } else {
                            builder.appendNull();
                        }
                    }
                    try (BytesRefBlock block = (BytesRefBlock) builder.build()) {
                        BytesRef scratch = new BytesRef();
                        for (int doc = 0; doc < numDocs; doc++) {
                            if (expected[doc] == null) {
                                assertTrue(block.isNull(doc));
                            } else {
                                assertThat(
                                    block.getBytesRef(block.getFirstValueIndex(doc), scratch).utf8ToString(),
                                    equalTo(expected[doc])
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
