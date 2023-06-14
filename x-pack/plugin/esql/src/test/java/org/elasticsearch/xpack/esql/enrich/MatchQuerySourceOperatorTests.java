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
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class MatchQuerySourceOperatorTests extends ESTestCase {

    public void testSingleTermsList() {
        BytesRefVector inputTerms = BytesRefVector.newVectorBuilder(7)
            .appendBytesRef(new BytesRef("b1")) // 0
            .appendBytesRef(new BytesRef("a2")) // 1
            .appendBytesRef(new BytesRef("a1")) // 2
            .appendBytesRef(new BytesRef("w1")) // 3
            .appendBytesRef(new BytesRef("c1")) // 4
            .appendBytesRef(new BytesRef("a1")) // 5
            .appendBytesRef(new BytesRef("b1")) // 6
            .build();
        MatchQuerySourceOperator.TermsList termsList = MatchQuerySourceOperator.buildTermsList(inputTerms.asBlock());
        BytesRef scratch = new BytesRef();
        assertThat(termsList.size(), equalTo(7));
        assertThat(termsList.getTerm(0, scratch), equalTo(new BytesRef("a1")));
        assertThat(termsList.getPosition(0), equalTo(2));
        assertThat(termsList.getTerm(1, scratch), equalTo(new BytesRef("a1")));
        assertThat(termsList.getPosition(1), equalTo(5));
        assertThat(termsList.getTerm(2, scratch), equalTo(new BytesRef("a2")));
        assertThat(termsList.getPosition(2), equalTo(1));
        assertThat(termsList.getTerm(3, scratch), equalTo(new BytesRef("b1")));
        assertThat(termsList.getPosition(3), equalTo(0));
        assertThat(termsList.getTerm(4, scratch), equalTo(new BytesRef("b1")));
        assertThat(termsList.getPosition(4), equalTo(6));
        assertThat(termsList.getTerm(5, scratch), equalTo(new BytesRef("c1")));
        assertThat(termsList.getPosition(5), equalTo(4));
        assertThat(termsList.getTerm(6, scratch), equalTo(new BytesRef("w1")));
        assertThat(termsList.getPosition(6), equalTo(3));
    }

    public void testMultiTermsList() {
        BytesRefBlock inputTerms = BytesRefBlock.newBlockBuilder(11)
            .appendBytesRef(new BytesRef("b1")) // 0
            .appendNull() // 1
            .beginPositionEntry() // 2
            .appendBytesRef(new BytesRef("a2"))
            .appendBytesRef(new BytesRef("a1"))
            .endPositionEntry()
            .appendBytesRef(new BytesRef("w1")) // 3
            .appendBytesRef(new BytesRef("c1")) // 4
            .appendNull() // 5
            .appendBytesRef(new BytesRef("a1")) // 6
            .appendNull() // 7
            .beginPositionEntry() // 8
            .appendBytesRef(new BytesRef("b1"))
            .endPositionEntry()
            .beginPositionEntry() // 9
            .appendBytesRef(new BytesRef("b1"))
            .appendBytesRef(new BytesRef("b2"))
            .endPositionEntry()
            .appendNull() // 11
            .build();
        MatchQuerySourceOperator.TermsList termsList = MatchQuerySourceOperator.buildTermsList(inputTerms);
        BytesRef scratch = new BytesRef();
        assertThat(termsList.size(), equalTo(9));
        assertThat(termsList.getTerm(0, scratch), equalTo(new BytesRef("a1")));
        assertThat(termsList.getPosition(0), equalTo(2));
        assertThat(termsList.getTerm(1, scratch), equalTo(new BytesRef("a1")));
        assertThat(termsList.getPosition(1), equalTo(6));
        assertThat(termsList.getTerm(2, scratch), equalTo(new BytesRef("a2")));
        assertThat(termsList.getPosition(2), equalTo(2));
        assertThat(termsList.getTerm(3, scratch), equalTo(new BytesRef("b1")));
        assertThat(termsList.getPosition(3), equalTo(0));
        assertThat(termsList.getTerm(4, scratch), equalTo(new BytesRef("b1")));
        assertThat(termsList.getPosition(4), equalTo(8));
        assertThat(termsList.getTerm(5, scratch), equalTo(new BytesRef("b1")));
        assertThat(termsList.getPosition(5), equalTo(9));
        assertThat(termsList.getTerm(6, scratch), equalTo(new BytesRef("b2")));
        assertThat(termsList.getPosition(6), equalTo(9));
        assertThat(termsList.getTerm(7, scratch), equalTo(new BytesRef("c1")));
        assertThat(termsList.getPosition(7), equalTo(4));
        assertThat(termsList.getTerm(8, scratch), equalTo(new BytesRef("w1")));
        assertThat(termsList.getPosition(8), equalTo(3));
    }

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

        BytesRefBlock inputTerms = BytesRefBlock.newBlockBuilder(5)
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

        MatchQuerySourceOperator queryOperator = new MatchQuerySourceOperator("uid", reader, inputTerms);
        Page page1 = queryOperator.getOutput();
        assertNotNull(page1);
        // pos -> terms -> docs
        // -----------------------------
        // 0 -> [b2] -> [1, 4]
        // 1 -> [c1, a2] -> [1, 0, 2]
        // 2 -> [z2] -> []
        // 3 -> [] -> []
        // 4 -> [a1] -> [3]
        // 5 -> [] -> []
        IntVector docs = ((DocBlock) page1.getBlock(0)).asVector().docs();
        IntBlock positions = page1.getBlock(1);
        assertThat(page1.getBlockCount(), equalTo(2));
        assertThat(page1.getPositionCount(), equalTo(6));
        int[] expectedDocs = new int[] { 0, 1, 1, 2, 3, 4 };
        int[] expectedPositions = new int[] { 1, 0, 1, 1, 4, 0 };
        for (int i = 0; i < page1.getPositionCount(); i++) {
            assertThat(docs.getInt(i), equalTo(expectedDocs[i]));
            assertThat(positions.getInt(i), equalTo(expectedPositions[i]));
        }
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
            doc.add(new StringField("id", term, Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer);
        writer.close();

        Map<Integer, Integer> expectedPositions = new HashMap<>();
        int numPositions = randomIntBetween(1, 1000);
        BytesRefBlock.Builder inputTerms = BytesRefBlock.newBlockBuilder(numPositions);
        for (int i = 0; i < numPositions; i++) {
            String term = randomFrom(terms.keySet());
            inputTerms.appendBytesRef(new BytesRef(term));
            expectedPositions.put(i, terms.get(term));
        }
        MatchQuerySourceOperator queryOperator = new MatchQuerySourceOperator("id", reader, inputTerms.build());
        Page page = queryOperator.getOutput();
        assertNotNull(page);
        assertThat(page.getPositionCount(), equalTo(numPositions));

        DocVector docBlock = ((DocBlock) page.getBlock(0)).asVector();
        IntVector docs = docBlock.docs();
        for (int i = 1; i < docs.getPositionCount(); i++) {
            assertThat("docs are not sorted ascending", docs.getInt(i), greaterThanOrEqualTo(docs.getInt(i - 1)));
        }
        Map<Integer, Integer> actualPositions = new HashMap<>();
        IntBlock positionBlock = page.getBlock(1);
        for (int i = 0; i < page.getPositionCount(); i++) {
            actualPositions.put(positionBlock.getInt(i), docs.getInt(i));
        }
        assertThat(actualPositions, equalTo(expectedPositions));
        IOUtils.close(reader, dir);
    }
}
