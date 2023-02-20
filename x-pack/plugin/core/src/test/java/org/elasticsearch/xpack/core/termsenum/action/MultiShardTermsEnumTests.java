/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum.action;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class MultiShardTermsEnumTests extends ESTestCase {

    public void testRandomIndexFusion() throws Exception {
        String fieldName = "foo";
        Set<String> globalTermCounts = new HashSet<>();

        int numShards = randomIntBetween(2, 15);

        ArrayList<Closeable> closeables = new ArrayList<>();
        ArrayList<DirectoryReader> readers = new ArrayList<>();

        try {
            for (int s = 0; s < numShards; s++) {
                Directory directory = new ByteBuffersDirectory();
                IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new MockAnalyzer(random())));

                int numDocs = randomIntBetween(10, 200);
                for (int i = 0; i < numDocs; i++) {
                    Document document = new Document();
                    String term = randomAlphaOfLengthBetween(1, 3).toLowerCase(Locale.ROOT);
                    document.add(new StringField(fieldName, term, Field.Store.YES));
                    writer.addDocument(document);
                    globalTermCounts.add(term);

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

                ArrayList<TransportTermsEnumAction.ShardTermsEnum> termsEnums = new ArrayList<>();
                for (DirectoryReader reader : readers) {
                    Terms terms = MultiTerms.getTerms(reader, fieldName);
                    TransportTermsEnumAction.ShardTermsEnum te = new TransportTermsEnumAction.ShardTermsEnum(
                        automaton.getTermsEnum(terms),
                        o -> ((BytesRef) o).utf8ToString()
                    );
                    if (randomBoolean()) {
                        // Simulate fields like constant-keyword which use a SimpleTermCountEnum to present results
                        // rather than the raw TermsEnum from Lucene.
                        ArrayList<String> termCounts = new ArrayList<>();
                        while (te.termsEnum().next() != null) {
                            termCounts.add(te.termsEnum().term().utf8ToString());
                        }
                        TransportTermsEnumAction.ShardTermsEnum simpleEnum = new TransportTermsEnumAction.ShardTermsEnum(
                            new SimpleTermCountEnum(termCounts.toArray(new String[0])),
                            o -> ((BytesRef) o).utf8ToString()
                        );
                        termsEnums.add(simpleEnum);
                    } else {
                        termsEnums.add(te);
                    }
                }
                MultiShardTermsEnum mte = new MultiShardTermsEnum(termsEnums.toArray(new TransportTermsEnumAction.ShardTermsEnum[0]));
                Set<String> expecteds = new HashSet<>();

                for (String term : globalTermCounts) {
                    if (term.startsWith(searchPrefix)) {
                        expecteds.add(term);
                    }
                }

                while (mte.next() != null) {
                    String teString = mte.decodedTerm();
                    assertTrue(expecteds.contains(teString));
                    expecteds.remove(teString);
                }
                assertEquals("Expected results not found", 0, expecteds.size());

            }
        } finally {
            IOUtils.close(closeables.toArray(new Closeable[0]));
        }
    }

}
