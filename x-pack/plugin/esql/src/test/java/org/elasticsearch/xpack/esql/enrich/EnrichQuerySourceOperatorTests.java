/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class EnrichQuerySourceOperatorTests extends ESTestCase {

    public void testQueries() throws Exception {
        MockDirectoryWrapper dir = newMockDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter writer = new IndexWriter(dir, iwc);
        List<List<String>> terms = List.of(
            List.of("a2"),
            List.of("a1", "c1", "b2"),
            List.of("a2"),
            List.of("a3"),
            List.of("b2", "b1", "a1")
        );
        for (List<String> ts : terms) {
            Document doc = new Document();
            for (String t : ts) {
                doc.add(new StringField("uid", t, Field.Store.NO));
            }
            writer.addDocument(doc);
        }
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer);
        writer.close();

        BytesRefBlock inputTerms = BytesRefBlock.newBlockBuilder(6)
            .appendBytesRef(new BytesRef("b2"))
            .beginPositionEntry()
            .appendBytesRef(new BytesRef("c1"))
            .appendBytesRef(new BytesRef("a2"))
            .endPositionEntry()
            .appendBytesRef(new BytesRef("z2"))
            .appendNull()
            .appendBytesRef(new BytesRef("a3"))
            .appendNull()
            .build();

        MappedFieldType uidField = new KeywordFieldMapper.KeywordFieldType("uid");
        QueryList queryList = QueryList.termQueryList(uidField, mock(SearchExecutionContext.class), inputTerms);
        assertThat(queryList.getPositionCount(), equalTo(6));
        assertThat(queryList.getQuery(0), equalTo(new TermQuery(new Term("uid", new BytesRef("b2")))));
        assertThat(queryList.getQuery(1), equalTo(new TermInSetQuery("uid", new BytesRef("c1"), new BytesRef("a2"))));
        assertThat(queryList.getQuery(2), equalTo(new TermQuery(new Term("uid", new BytesRef("z2")))));
        assertNull(queryList.getQuery(3));
        assertThat(queryList.getQuery(4), equalTo(new TermQuery(new Term("uid", new BytesRef("a3")))));
        assertNull(queryList.getQuery(5));
        // pos -> terms -> docs
        // -----------------------------
        // 0 -> [b2] -> [1, 4]
        // 1 -> [c1, a2] -> [1, 0, 2]
        // 2 -> [z2] -> []
        // 3 -> [] -> []
        // 4 -> [a1] -> [3]
        // 5 -> [] -> []
        EnrichQuerySourceOperator queryOperator = new EnrichQuerySourceOperator(queryList, reader);
        {
            Page p0 = queryOperator.getOutput();
            assertNotNull(p0);
            assertThat(p0.getPositionCount(), equalTo(2));
            IntVector docs = getDocVector(p0, 0);
            assertThat(docs.getInt(0), equalTo(1));
            assertThat(docs.getInt(1), equalTo(4));
            Block positions = p0.getBlock(1);
            assertThat(BlockUtils.toJavaObject(positions, 0), equalTo(0));
            assertThat(BlockUtils.toJavaObject(positions, 1), equalTo(0));
        }
        {
            Page p1 = queryOperator.getOutput();
            assertNotNull(p1);
            assertThat(p1.getPositionCount(), equalTo(3));
            IntVector docs = getDocVector(p1, 0);
            assertThat(docs.getInt(0), equalTo(0));
            assertThat(docs.getInt(1), equalTo(1));
            assertThat(docs.getInt(2), equalTo(2));
            Block positions = p1.getBlock(1);
            assertThat(BlockUtils.toJavaObject(positions, 0), equalTo(1));
            assertThat(BlockUtils.toJavaObject(positions, 1), equalTo(1));
            assertThat(BlockUtils.toJavaObject(positions, 2), equalTo(1));
        }
        {
            Page p2 = queryOperator.getOutput();
            assertNull(p2);
        }
        {
            Page p3 = queryOperator.getOutput();
            assertNull(p3);
        }
        {
            Page p4 = queryOperator.getOutput();
            assertNotNull(p4);
            assertThat(p4.getPositionCount(), equalTo(1));
            IntVector docs = getDocVector(p4, 0);
            assertThat(docs.getInt(0), equalTo(3));
            Block positions = p4.getBlock(1);
            assertThat(BlockUtils.toJavaObject(positions, 0), equalTo(4));
        }
        {
            Page p5 = queryOperator.getOutput();
            assertNull(p5);
        }
        {
            assertFalse(queryOperator.isFinished());
            Page p6 = queryOperator.getOutput();
            assertNull(p6);
        }
        assertTrue(queryOperator.isFinished());
        IOUtils.close(reader, dir);
    }

    public void testRandomMatchQueries() throws Exception {
        MockDirectoryWrapper dir = newMockDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter writer = new IndexWriter(dir, iwc);
        int numTerms = randomIntBetween(10, 1000);
        Map<String, Integer> terms = new HashMap<>();
        for (int i = 0; i < numTerms; i++) {
            Document doc = new Document();
            String term = "term-" + i;
            terms.put(term, i);
            doc.add(new StringField("uid", term, Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer);
        writer.close();

        Map<Integer, Set<Integer>> expectedPositions = new HashMap<>();
        int numPositions = randomIntBetween(1, 1000);
        BytesRefBlock.Builder inputTerms = BytesRefBlock.newBlockBuilder(numPositions);
        for (int i = 0; i < numPositions; i++) {
            if (randomBoolean()) {
                String term = randomFrom(terms.keySet());
                inputTerms.appendBytesRef(new BytesRef(term));
                Integer position = terms.get(term);
                expectedPositions.put(i, Set.of(position));
            } else {
                if (randomBoolean()) {
                    inputTerms.appendNull();
                } else {
                    String term = "other-" + randomIntBetween(1, 100);
                    inputTerms.appendBytesRef(new BytesRef(term));
                }
            }
        }
        MappedFieldType uidField = new KeywordFieldMapper.KeywordFieldType("uid");
        QueryList queryList = QueryList.termQueryList(uidField, mock(SearchExecutionContext.class), inputTerms.build());
        EnrichQuerySourceOperator queryOperator = new EnrichQuerySourceOperator(queryList, reader);
        Map<Integer, Set<Integer>> actualPositions = new HashMap<>();
        while (queryOperator.isFinished() == false) {
            Page page = queryOperator.getOutput();
            if (page != null) {
                IntVector docs = getDocVector(page, 0);
                IntBlock positions = page.getBlock(1);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    int doc = docs.getInt(i);
                    int position = positions.getInt(i);
                    actualPositions.computeIfAbsent(position, k -> new HashSet<>()).add(doc);
                }
            }
        }
        assertThat(actualPositions, equalTo(expectedPositions));
        IOUtils.close(reader, dir);
    }

    private static IntVector getDocVector(Page page, int blockIndex) {
        DocBlock doc = page.getBlock(blockIndex);
        return doc.asVector().docs();
    }
}
