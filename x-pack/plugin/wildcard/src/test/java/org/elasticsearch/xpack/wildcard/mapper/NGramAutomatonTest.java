/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.wildcard.mapper.regex.And;
import org.elasticsearch.xpack.wildcard.mapper.regex.AutomatonTooComplexException;
import org.elasticsearch.xpack.wildcard.mapper.regex.Expression;
import org.elasticsearch.xpack.wildcard.mapper.regex.Leaf;
import org.elasticsearch.xpack.wildcard.mapper.regex.Or;
import org.elasticsearch.xpack.wildcard.mapper.regex.True;
import org.junit.Test;

import static org.elasticsearch.xpack.wildcard.mapper.regex.Leaf.leaves;

public class NGramAutomatonTest extends ESTestCase {
    @Test
    public void simple() {
        assertTrigramExpression("cat", new Leaf<>("cat"));
    }

    @Test
    public void options() {
        assertTrigramExpression("(cat)|(dog)|(cow)", new Or<String>(leaves("cat", "dog", "cow")));
    }

    @Test
    public void leadingWildcard() {
        assertTrigramExpression(".*cat", new Leaf<>("cat"));
    }

    @Test
    public void followingWildcard() {
        assertTrigramExpression("cat.*", new Leaf<>("cat"));
    }

    @Test
    public void initialCharClassExpanded() {
        assertTrigramExpression("[abcd]oop", new And<String>(new Or<String>(leaves("aoo", "boo", "coo", "doo")), new Leaf<>("oop")));
    }

    @Test
    public void initialCharClassSkipped() {
        assertTrigramExpression("[abcde]oop", new Leaf<String>("oop"));
    }

    @Test
    public void followingCharClassExpanded() {
        assertTrigramExpression("oop[abcd]", new And<String>(
                new Leaf<>("oop"),
                new Or<String>(leaves("opa", "opb", "opc", "opd"))));
    }

    @Test
    public void followingCharClassSkipped() {
        assertTrigramExpression("oop[abcde]", new Leaf<String>("oop"));
    }

    @Test
    public void shortCircuit() {
        assertTrigramExpression("a|(lopi)", True.<String> instance());
    }

    @Test
    public void optional() {
        assertTrigramExpression("(a|[j-t])lopi", new And<String>(leaves("lop", "opi")));
    }

    @Test
    public void loop() {
        assertTrigramExpression("ab(cdef)*gh", new Or<String>(
                new And<String>(leaves("abc", "bcd", "cde", "def", "efg", "fgh")),
                new And<String>(leaves("abg", "bgh"))));
    }

    @Test
    public void converge() {
        assertTrigramExpression("(ajdef)|(cdef)", new And<String>(
                new Or<String>(
                        new And<String>(leaves("ajd", "jde")),
                        new Leaf<>("cde")),
                        new Leaf<>("def")));
    }

    @Test
    public void complex() {
        assertTrigramExpression("h[efas] te.*me", new And<String>(
                new Or<String>(
                        new And<String>(leaves("ha ", "a t")),
                        new And<String>(leaves("he ", "e t")),
                        new And<String>(leaves("hf ", "f t")),
                        new And<String>(leaves("hs ", "s t"))),
                new Leaf<>(" te")));
    }

    // The pgTrgmExample methods below test examples from the slides at
    // http://www.pgcon.org/2012/schedule/attachments/248_Alexander%20Korotkov%20-%20Index%20support%20for%20regular%20expression%20search.pdf
    @Test
    public void pgTrgmExample1() {
        assertTrigramExpression("a(b+|c+)d", new Or<String>(
                new Leaf<>("abd"),
                new And<String>(leaves("abb", "bbd")),
                new Leaf<>("acd"),
                new And<String>(leaves("acc", "ccd"))));
    }

    @Test
    public void pgTrgmExample2() {
        assertTrigramExpression("(abc|cba)def", new And<String>(
                new Leaf<>("def"), new Or<String>(
                        new And<String>(leaves("abc", "bcd", "cde")),
                        new And<String>(leaves("cba", "bad", "ade")))));
    }

    @Test
    public void pgTrgmExample3() {
        assertTrigramExpression("abc+de", new And<String>(
                new Leaf<>("abc"),
                new Leaf<>("cde"),
                new Or<String>(
                        new Leaf<>("bcd"),
                        new And<String>(leaves("bcc", "ccd")))));
    }

    @Test
    public void pgTrgmExample4() {
        assertTrigramExpression("(abc*)+de", new Or<String>(
                new And<String>(leaves("abd", "bde")),
                new And<String>(
                        new Leaf<>("abc"),
                        new Leaf<>("cde"),
                        new Or<String>(
                                new Leaf<>("bcd"),
                                new And<String>(leaves("bcc", "ccd"))))));
    }

    @Test
    public void pgTrgmExample5() {
        assertTrigramExpression("ab(cd)*ef", new Or<String>(
                new And<String>(leaves("abe", "bef")),
                new And<String>(leaves("abc", "bcd", "cde", "def"))));
    }

    /**
     * Automatons that would take too long to process are aborted.
     */
//    @Test(expected=AutomatonTooComplexException.class)
    public void tooManyStates() {
        // TODO I'm not sure how to reliably trigger this without really high maxTransitions.  Maybe its not possible?
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            b.append("[efas]+");
        }
        assertTrigramExpression(b.toString(), null /*ignored*/);
    }

    /**
     * This would periodically fail when we were removing cycles rather
     * preventing them from being added to the expression.
     */
    @Test
    public void bigramFailsSometimes() {
        assertExpression("te.*me", 2, new And<String>(leaves("te", "me")));
    }

    @Test(expected=TooComplexToDeterminizeException.class)
    public void tooBig() {
        assertTrigramExpression("\\[\\[(Datei|File|Bild|Image):[^]]*alt=[^]|}]{50,200}",
                null /* ignored */);
    }

    @Test(expected=TooComplexToDeterminizeException.class)
    public void tooBigToo() {
        assertTrigramExpression("[^]]*alt=[^]\\|}]{80,}",
                null /* ignored */);
    }

    @Test
    public void huge() {
        assertTrigramExpression("[ac]*a[de]{50,80}", null);
    }

    @Test
    @Repeat(iterations=100)
    public void randomRegexp() {
        // Some of the regex strings don't actually compile so just retry until we get a good one.
        String str;
        while (true) {
            try {
                str = AutomatonTestUtil.randomRegexp(random());
                new RegExp(str);
                break;
            } catch (Exception e) {
                // retry now
            }
        }
        assertTrigramExpression(str, null);
    }

    /**
     * Tests that building the automaton doesn't blow up in unexpected ways.
     */
    @Test
    @Repeat(iterations=100)
    public void randomAutomaton() {
        Automaton automaton = AutomatonTestUtil.randomAutomaton(random());
        NGramAutomaton ngramAutomaton;
        try {
            ngramAutomaton = new NGramAutomaton(automaton, between(2, 7), 4, 10000, 500);
        } catch (AutomatonTooComplexException e) {
            // This is fine - some automata are genuinely too complex to ngramify.
            return;
        }
        Expression<String> expression = ngramAutomaton.expression();
        expression = expression.simplify();
    }

    /**
     * Asserts that the provided regex extracts the expected expression when
     * configured to extract trigrams. Uses 4 as maxExpand just because I had to
     * pick something and 4 seemed pretty good.
     */
    private void assertTrigramExpression(String regex, Expression<String> expected) {
        assertExpression(regex, 3, expected);
    }

    /**
     * Asserts that the provided regex extracts the expected expression when
     * configured to extract ngrams. Uses 4 as maxExpand just because I had to
     * pick something and 4 seemed pretty good.
     */
    private void assertExpression(String regex, int gramSize, Expression<String> expected) {
//         System.err.println(regex);
        Automaton automaton = new RegExp(regex).toAutomaton(20000);
//         System.err.println(automaton.toDot());
        NGramAutomaton ngramAutomaton = new NGramAutomaton(automaton, gramSize, 4, 10000, 100);
//         System.err.println(ngramAutomaton.toDot());
        Expression<String> expression = ngramAutomaton.expression();
//         System.err.println(expression);
        expression = expression.simplify();
//         System.err.println(expression);
        if (expected != null) {
            // Null means skip the test here.
            assertEquals(expected, expression);
        }
    }
}
