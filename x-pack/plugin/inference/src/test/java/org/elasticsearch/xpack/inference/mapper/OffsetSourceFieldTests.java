/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;

public class OffsetSourceFieldTests extends ESTestCase {
    public void testBasics() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(
            random(),
            dir,
            newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean()))
        );
        Document doc = new Document();
        OffsetSourceField field1 = new OffsetSourceField("field1", "foo", 1, 10);
        doc.add(field1);
        writer.addDocument(doc);

        field1.setValues("bar", 10, 128);
        writer.addDocument(doc);

        writer.addDocument(new Document()); // gap

        field1.setValues("foo", 50, 256);
        writer.addDocument(doc);

        writer.addDocument(new Document()); // double gap
        writer.addDocument(new Document());

        field1.setValues("baz", 32, 512);
        writer.addDocument(doc);

        writer.forceMerge(1);
        var reader = writer.getReader();
        writer.close();

        var searcher = newSearcher(reader);
        var context = searcher.getIndexReader().leaves().get(0);

        var terms = context.reader().terms("field1");
        assertNotNull(terms);
        OffsetSourceField.OffsetSourceLoader loader = OffsetSourceField.loader(terms);

        var offset = loader.advanceTo(0);
        assertEquals(new OffsetSourceFieldMapper.OffsetSource("foo", 1, 10), offset);

        offset = loader.advanceTo(1);
        assertEquals(new OffsetSourceFieldMapper.OffsetSource("bar", 10, 128), offset);

        assertNull(loader.advanceTo(2));

        offset = loader.advanceTo(3);
        assertEquals(new OffsetSourceFieldMapper.OffsetSource("foo", 50, 256), offset);

        offset = loader.advanceTo(6);
        assertEquals(new OffsetSourceFieldMapper.OffsetSource("baz", 32, 512), offset);

        assertNull(loader.advanceTo(189));

        IOUtils.close(reader, dir);
    }
}
