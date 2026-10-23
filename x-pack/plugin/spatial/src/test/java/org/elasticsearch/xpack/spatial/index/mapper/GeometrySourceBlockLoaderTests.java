/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class GeometrySourceBlockLoaderTests extends ESTestCase {

    /**
     * The reader wraps a forward-only binary doc values iterator, so {@code canReuse} has to track the last document read. Reporting
     * {@code true} for an earlier document lets {@code ValuesSourceReaderOperator} hand a retained reader a document it has already
     * passed, which resolves against whichever binary doc values block is still loaded.
     */
    public void testCanReuseTracksLastReadDoc() throws IOException {
        String field = GeometrySourceDocValuesField.fieldName("location");
        var loader = new GeometrySourceBlockLoader(field);
        try (Directory directory = newDirectory()) {
            try (var iw = new IndexWriter(directory, new IndexWriterConfig(null /* analyzer */))) {
                for (int i = 0; i < 5; i++) {
                    var dv = new GeometrySourceDocValuesField(field);
                    dv.add(new Point(i, i + 1));
                    var doc = new Document();
                    doc.add(dv);
                    iw.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                var leaf = getOnlyLeafReader(reader).getContext();
                CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
                try (BlockLoader.ColumnAtATimeReader allReader = loader.reader(breaker, leaf)) {
                    allReader.read(TestBlock.factory(), TestBlock.docs(3), 0, false).close();

                    assertFalse("must not be reused for an earlier doc", allReader.canReuse(2));
                    assertTrue("may be reused for the doc it just read", allReader.canReuse(3));
                    assertTrue("may be reused for a later doc", allReader.canReuse(4));
                }
            }
        }
    }
}
