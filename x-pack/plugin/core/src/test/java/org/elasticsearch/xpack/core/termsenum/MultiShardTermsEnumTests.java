/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.termsenum.action.MultiShardTermsEnum;
import org.elasticsearch.xpack.core.termsenum.action.SimpleTermCountEnum;
import org.elasticsearch.xpack.core.termsenum.action.TermCount;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

public class MultiShardTermsEnumTests extends ESTestCase {
    
    public void testRandomIndexFusion() throws Exception {
        String fieldName = "foo";
        Map<String, Integer> globalTermCounts = new HashMap<>();

        int numShards = randomIntBetween(2, 15);

        ArrayList<Closeable> closeables = new ArrayList<>();
        ArrayList<DirectoryReader> readers = new ArrayList<>();

        try {
            for (int s = 0; s < numShards; s++) {
                Directory directory = new ByteBuffersDirectory();
                IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new MockAnalyzer(random())));

                int numDocs = randomIntBetween(10,200);
                for (int i = 0; i < numDocs; i++) {
                    Document document = new Document();
                    String term = randomAlphaOfLengthBetween(1,3).toLowerCase(Locale.ROOT);
                    document.add(new StringField(fieldName, term, Field.Store.YES));
                    writer.addDocument(document);
                    int count = 0;
                    if (globalTermCounts.containsKey(term)) {
                        count = globalTermCounts.get(term);
                    }
                    count++;
                    globalTermCounts.put(term, count);

                }
                DirectoryReader reader = DirectoryReader.open(writer);
                readers.add(reader);
                writer.close();
                closeables.add(reader);
                closeables.add(directory);
            }

            int numSearches = 100;
            for (int q = 0; q < numSearches; q++) {
                String searchPrefix = randomAlphaOfLengthBetween(0, 3).toLowerCase(Locale.ROOT);
                Automaton a = AutomatonQueries.caseInsensitivePrefix(searchPrefix);
                a = Operations.concatenate(a, Automata.makeAnyString());
                a = MinimizationOperations.minimize(a, Integer.MAX_VALUE);
                CompiledAutomaton automaton = new CompiledAutomaton(a);

                ArrayList<TermsEnum> termsEnums = new ArrayList<>();
                for (DirectoryReader reader : readers) {
                    Terms terms = MultiTerms.getTerms(reader, fieldName);
                    TermsEnum te = automaton.getTermsEnum(terms);
                    if (randomBoolean()) {
                        // Simulate fields like constant-keyword which use a SimpleTermCountEnum to present results
                        // rather than the raw TermsEnum from Lucene.
                        ArrayList<TermCount> termCounts = new ArrayList<>();
                        while (te.next() != null) {
                            termCounts.add(new TermCount(te.term().utf8ToString(), te.docFreq()));
                        }
                        SimpleTermCountEnum simpleEnum = new SimpleTermCountEnum(termCounts.toArray(new TermCount[0]));
                        termsEnums.add(simpleEnum);
                    } else {
                        termsEnums.add(te);
                    }
                }
                MultiShardTermsEnum mte = new MultiShardTermsEnum(termsEnums.toArray(new TermsEnum[0]));
                HashMap<String, Integer> expecteds = new HashMap<>();

                for (Entry<String, Integer> termCount : globalTermCounts.entrySet()) {
                    if (termCount.getKey().startsWith(searchPrefix)) {
                        expecteds.put(termCount.getKey(), termCount.getValue());
                    }
                }
                
                while (mte.next() != null) {
                    String teString = mte.term().utf8ToString();
                    long actual = mte.docFreq();
                    assertTrue(expecteds.containsKey(teString));
                    long expected = expecteds.get(teString);
                    expecteds.remove(teString);
                    assertEquals(mte.term().utf8ToString() + " string count wrong", expected, actual);
                }
                assertEquals("Expected results not found", 0, expecteds.size());

            }
        } finally {
            IOUtils.close(closeables.toArray(new Closeable[0]));
        }
    }

}
