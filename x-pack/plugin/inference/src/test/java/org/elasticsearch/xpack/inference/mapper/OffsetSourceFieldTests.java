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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

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

        var offset = loader.advanceTo(0, IndexVersion.current());
        assertEquals(new OffsetSourceFieldMapper.OffsetSource("foo", 1, 10), offset);

        offset = loader.advanceTo(1, IndexVersion.current());
        assertEquals(new OffsetSourceFieldMapper.OffsetSource("bar", 10, 128), offset);

        assertNull(loader.advanceTo(2, IndexVersion.current()));

        offset = loader.advanceTo(3, IndexVersion.current());
        assertEquals(new OffsetSourceFieldMapper.OffsetSource("foo", 50, 256), offset);

        offset = loader.advanceTo(6, IndexVersion.current());
        assertEquals(new OffsetSourceFieldMapper.OffsetSource("baz", 32, 512), offset);

        assertNull(loader.advanceTo(189, IndexVersion.current()));

        IOUtils.close(reader, dir);
    }

    public void testInputIndex() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(
            random(),
            dir,
            newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean()))
        );

        Document doc = new Document();
        OffsetSourceField field = new OffsetSourceField("field1", "foo", 0);
        doc.add(field);
        writer.addDocument(doc);

        // Offset-bearing doc interleaved with input-index-bearing docs.
        field.setValues("bar", 5, 42);
        writer.addDocument(doc);

        field.setValues("foo", 7);
        writer.addDocument(doc);

        writer.addDocument(new Document()); // gap

        field.setValues("baz", 123);
        writer.addDocument(doc);

        writer.forceMerge(1);
        var reader = writer.getReader();
        writer.close();

        var searcher = newSearcher(reader);
        var context = searcher.getIndexReader().leaves().get(0);

        var terms = context.reader().terms("field1");
        assertNotNull(terms);
        OffsetSourceField.OffsetSourceLoader loader = OffsetSourceField.loader(terms);

        assertEquals(new OffsetSourceFieldMapper.OffsetSource("foo", 0), loader.advanceTo(0, IndexVersion.current()));
        assertEquals(new OffsetSourceFieldMapper.OffsetSource("bar", 5, 42), loader.advanceTo(1, IndexVersion.current()));
        assertEquals(new OffsetSourceFieldMapper.OffsetSource("foo", 7), loader.advanceTo(2, IndexVersion.current()));
        assertNull(loader.advanceTo(3, IndexVersion.current()));
        assertEquals(new OffsetSourceFieldMapper.OffsetSource("baz", 123), loader.advanceTo(4, IndexVersion.current()));

        IOUtils.close(reader, dir);
    }

    public void testOffsetSentinel() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(
            random(),
            dir,
            newIndexWriterConfig().setMergePolicy(newLogMergePolicy(random().nextBoolean()))
        );

        // Write a doc via the offset-form path with (0, 0) offsets. Under pre-SEMANTIC_FIELD_TYPE indices this is a legitimate zero-length
        // span; under SEMANTIC_FIELD_TYPE and later this is the sentinel that flags the posting as carrying an inputIndex instead.
        Document doc = new Document();
        doc.add(new OffsetSourceField("field1", "term", 0, 0));
        writer.addDocument(doc);

        writer.forceMerge(1);
        var reader = writer.getReader();
        writer.close();

        var searcher = newSearcher(reader);
        var context = searcher.getIndexReader().leaves().get(0);
        var terms = context.reader().terms("field1");
        assertNotNull(terms);

        for (int i = 0; i < 20; i++) {
            // Pre-sentinel: any supported index version strictly before SEMANTIC_FIELD_TYPE reads (0, 0) as an offset span.
            IndexVersion preVersion = IndexVersionUtils.randomPreviousCompatibleVersion(IndexVersions.SEMANTIC_FIELD_TYPE);
            OffsetSourceField.OffsetSourceLoader preLoader = OffsetSourceField.loader(terms);
            assertEquals(new OffsetSourceFieldMapper.OffsetSource("term", 0, 0), preLoader.advanceTo(0, preVersion));

            // Post-sentinel: any version at or after SEMANTIC_FIELD_TYPE interprets (0, 0) as the inputIndex sentinel. The default
            // PositionIncrementAttribute of 1 lands the token at absolute position 0, so inputIndex == 0.
            IndexVersion postVersion = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.SEMANTIC_FIELD_TYPE);
            OffsetSourceField.OffsetSourceLoader postLoader = OffsetSourceField.loader(terms);
            assertEquals(new OffsetSourceFieldMapper.OffsetSource("term", 0), postLoader.advanceTo(0, postVersion));
        }

        IOUtils.close(reader, dir);
    }
}
