/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.wildcard.mapper.regex.And;
import org.elasticsearch.xpack.wildcard.mapper.regex.Leaf;
import org.elasticsearch.xpack.wildcard.mapper.regex.True;

import static org.elasticsearch.xpack.wildcard.mapper.regex.Leaf.leaves;

public class NGramExtractorTest extends ESTestCase {
    public void testSimple() {
        NGramExtractor gram = new NGramExtractor(3, 4, 10000, 100);
        Automaton automaton = new RegExp("hero of legend").toAutomaton();
        assertEquals(
                new And<String>(leaves("her", "ero", "ro ", "o o", " of",
                        "of ", "f l", " le", "leg", "ege", "gen", "end")),
                gram.extract(automaton));
        automaton = new RegExp("").toAutomaton();
        assertEquals(True.<String> instance(), gram.extract(automaton));
        automaton = new RegExp(".*").toAutomaton();
        assertEquals(True.<String> instance(), gram.extract(automaton));
        automaton = new RegExp("he").toAutomaton();
        assertEquals(True.<String> instance(), gram.extract(automaton));
        automaton = new RegExp("her").toAutomaton();
        assertEquals(new Leaf<>("her"), gram.extract(automaton));
    }

    public void testMaxNgrams() {
        NGramExtractor gram = new NGramExtractor(3, 4, 10000, 3);
        Automaton automaton = new RegExp("hero of legend").toAutomaton();
        assertEquals(
                new And<String>(leaves("her", "ero", "ro ")),
                gram.extract(automaton));
    }
}
