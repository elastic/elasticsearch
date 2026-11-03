/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.perfield;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@link XPerFieldDocValuesFormat} is a fork of {@link PerFieldDocValuesFormat} that widens access to one field. These assert the
 * fork is otherwise interchangeable: it names its per-field attributes with Lucene's own keys, and its own name never reaches a
 * segment, so the two must produce the same files and the same values.
 */
public class XPerFieldDocValuesFormatDuelTests extends ESTestCase {

    private static final int NUM_DOCS = 200;

    private static Codec codec(DocValuesFormat docValuesFormat) {
        // Both sides write under the real codec name, so reads resolve through the SPI codec and its Lucene wrapper. That is the
        // property being checked: what the fork writes has to be readable by Lucene's wrapper.
        return new FilterCodec("Elasticsearch96", new Lucene104Codec()) {
            @Override
            public DocValuesFormat docValuesFormat() {
                return docValuesFormat;
            }
        };
    }

    private static Codec luceneCodec() {
        final DocValuesFormat inner = new Lucene90DocValuesFormat();
        return codec(new PerFieldDocValuesFormat() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return inner;
            }
        });
    }

    private static Codec forkCodec() {
        final DocValuesFormat inner = new Lucene90DocValuesFormat();
        return codec(new XPerFieldDocValuesFormat() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return inner;
            }
        });
    }

    private static void index(Directory dir, Codec codec) throws Exception {
        IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE).setUseCompoundFile(false);
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < NUM_DOCS; i++) {
                Document doc = new Document();
                doc.add(new NumericDocValuesField("numeric", i));
                doc.add(new SortedNumericDocValuesField("sorted_numeric", i % 7));
                doc.add(new SortedDocValuesField("sorted", new BytesRef("term-" + (i % 13))));
                doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("set-" + (i % 5))));
                doc.add(new BinaryDocValuesField("binary", new BytesRef("bin-" + i)));
                w.addDocument(doc);
            }
            w.commit();
        }
    }

    /**
     * The files the codec wrote. The test framework picks a directory implementation at random for each call, and they do not all
     * leave the same bookkeeping behind.
     */
    private static Map<String, Long> indexFiles(Directory dir) throws IOException {
        Map<String, Long> files = new TreeMap<>();
        for (String file : dir.listAll()) {
            if (file.equals(IndexWriter.WRITE_LOCK_NAME) || file.startsWith("extra")) {
                continue;
            }
            files.put(file, dir.fileLength(file));
        }
        return files;
    }

    /** Same field data through either wrapper has to produce the same files, at the same sizes. */
    public void testBothWrappersProduceTheSameFiles() throws Exception {
        try (Directory lucene = newDirectory(); Directory fork = newDirectory()) {
            index(lucene, luceneCodec());
            index(fork, forkCodec());

            assertEquals("the wrappers must write the same files at the same sizes", indexFiles(lucene), indexFiles(fork));
        }
    }

    /** And the values have to read back identically. */
    public void testBothWrappersReadBackTheSameValues() throws Exception {
        try (Directory lucene = newDirectory(); Directory fork = newDirectory()) {
            index(lucene, luceneCodec());
            index(fork, forkCodec());

            try (DirectoryReader luceneReader = DirectoryReader.open(lucene); DirectoryReader forkReader = DirectoryReader.open(fork)) {
                assertEquals(1, luceneReader.leaves().size());
                assertEquals(1, forkReader.leaves().size());
                LeafReader a = luceneReader.leaves().get(0).reader();
                LeafReader b = forkReader.leaves().get(0).reader();

                for (int doc = 0; doc < NUM_DOCS; doc++) {
                    var an = a.getNumericDocValues("numeric");
                    var bn = b.getNumericDocValues("numeric");
                    assertTrue(an.advanceExact(doc));
                    assertTrue(bn.advanceExact(doc));
                    assertEquals("numeric doc " + doc, an.longValue(), bn.longValue());

                    var as = a.getSortedDocValues("sorted");
                    var bs = b.getSortedDocValues("sorted");
                    assertTrue(as.advanceExact(doc));
                    assertTrue(bs.advanceExact(doc));
                    assertEquals("sorted doc " + doc, as.lookupOrd(as.ordValue()), bs.lookupOrd(bs.ordValue()));

                    var ab = a.getBinaryDocValues("binary");
                    var bb = b.getBinaryDocValues("binary");
                    assertTrue(ab.advanceExact(doc));
                    assertTrue(bb.advanceExact(doc));
                    assertEquals("binary doc " + doc, ab.binaryValue(), bb.binaryValue());
                }
            }
        }
    }
}
