/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.analysis.synonym;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_MAX_DETERMINIZED_STATES;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.FiniteStringsIterator;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.Transition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates a list of {@link TokenStream} where each stream is the tokens that make up a finite string in graph token stream.  To do this,
 * the graph token stream is converted to an {@link Automaton} and from there we use a {@link FiniteStringsIterator} to collect the various
 * token streams for each finite string.
 */
public class GraphTokenStreamFiniteStrings {
    private final Automaton.Builder builder;
    Automaton det;
    private final Map<BytesRef, Integer> termToID = new HashMap<>();
    private final Map<Integer, BytesRef> idToTerm = new HashMap<>();
    private int anyTermID = -1;

    public GraphTokenStreamFiniteStrings() {
        this.builder = new Automaton.Builder();
    }

    private static class BytesRefArrayTokenStream extends TokenStream {
        private final BytesTermAttribute termAtt = addAttribute(BytesTermAttribute.class);
        private final BytesRef[] terms;
        private int offset;

        BytesRefArrayTokenStream(BytesRef[] terms) {
            this.terms = terms;
            offset = 0;
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (offset < terms.length) {
                clearAttributes();
                termAtt.setBytesRef(terms[offset]);
                offset = offset + 1;
                return true;
            }

            return false;
        }
    }

    /**
     * Gets
     */
    public List<TokenStream> getTokenStreams(final TokenStream in) throws IOException {
        // build automation
        build(in);

        List<TokenStream> tokenStreams = new ArrayList<>();
        final FiniteStringsIterator finiteStrings = new FiniteStringsIterator(det);
        for (IntsRef string; (string = finiteStrings.next()) != null; ) {
            final BytesRef[] tokens = new BytesRef[string.length];
            for (int idx = string.offset, len = string.offset + string.length; idx < len; idx++) {
                tokens[idx - string.offset] = idToTerm.get(string.ints[idx]);
            }

            tokenStreams.add(new BytesRefArrayTokenStream(tokens));
        }

        return tokenStreams;
    }

    private void build(final TokenStream in) throws IOException {
        if (det != null) {
            throw new IllegalStateException("Automation already built");
        }

        final TermToBytesRefAttribute termBytesAtt = in.addAttribute(TermToBytesRefAttribute.class);
        final PositionIncrementAttribute posIncAtt = in.addAttribute(PositionIncrementAttribute.class);
        final PositionLengthAttribute posLengthAtt = in.addAttribute(PositionLengthAttribute.class);
        final OffsetAttribute offsetAtt = in.addAttribute(OffsetAttribute.class);

        in.reset();

        int pos = -1;
        int lastPos = 0;
        int maxOffset = 0;
        int maxPos = -1;
        int state = -1;
        while (in.incrementToken()) {
            int posInc = posIncAtt.getPositionIncrement();
            assert pos > -1 || posInc > 0;

            if (posInc > 1) {
                throw new IllegalArgumentException("cannot handle holes; to accept any term, use '*' term");
            }

            if (posInc > 0) {
                // New node:
                pos += posInc;
            }

            int endPos = pos + posLengthAtt.getPositionLength();
            while (state < endPos) {
                state = createState();
            }

            BytesRef term = termBytesAtt.getBytesRef();
            //System.out.println(pos + "-" + endPos + ": " + term.utf8ToString() + ": posInc=" + posInc);
            if (term.length == 1 && term.bytes[term.offset] == (byte) '*') {
                addAnyTransition(pos, endPos);
            } else {
                addTransition(pos, endPos, term);
            }

            maxOffset = Math.max(maxOffset, offsetAtt.endOffset());
            maxPos = Math.max(maxPos, endPos);
        }

        in.end();

        // TODO: look at endOffset?  ts2a did...

        // TODO: this (setting "last" state as the only accept state) may be too simplistic?
        setAccept(state, true);
        finish();
    }

    /**
     * Returns a new state; state 0 is always the initial state.
     */
    private int createState() {
        return builder.createState();
    }

    /**
     * Marks the specified state as accept or not.
     */
    private void setAccept(int state, boolean accept) {
        builder.setAccept(state, accept);
    }

    /**
     * Adds a transition to the automaton.
     */
    private void addTransition(int source, int dest, String term) {
        addTransition(source, dest, new BytesRef(term));
    }

    /**
     * Adds a transition to the automaton.
     */
    private void addTransition(int source, int dest, BytesRef term) {
        if (term == null) {
            throw new NullPointerException("term should not be null");
        }
        builder.addTransition(source, dest, getTermID(term));
    }

    /**
     * Adds a transition matching any term.
     */
    private void addAnyTransition(int source, int dest) {
        builder.addTransition(source, dest, getTermID(null));
    }

    /**
     * Call this once you are done adding states/transitions.
     */
    private void finish() {
        finish(DEFAULT_MAX_DETERMINIZED_STATES);
    }

    /**
     * Call this once you are done adding states/transitions.
     *
     * @param maxDeterminizedStates Maximum number of states created when determinizing the automaton.  Higher numbers allow this operation
     *                              to consume more memory but allow more complex automatons.
     */
    private void finish(int maxDeterminizedStates) {
        Automaton automaton = builder.finish();

        // System.out.println("before det:\n" + automaton.toDot());

        Transition t = new Transition();

        // TODO: should we add "eps back to initial node" for all states,
        // and det that?  then we don't need to revisit initial node at
        // every position?  but automaton could blow up?  And, this makes it
        // harder to skip useless positions at search time?

        if (anyTermID != -1) {

            // Make sure there are no leading or trailing ANY:
            int count = automaton.initTransition(0, t);
            for (int i = 0; i < count; i++) {
                automaton.getNextTransition(t);
                if (anyTermID >= t.min && anyTermID <= t.max) {
                    throw new IllegalStateException("automaton cannot lead with an ANY transition");
                }
            }

            int numStates = automaton.getNumStates();
            for (int i = 0; i < numStates; i++) {
                count = automaton.initTransition(i, t);
                for (int j = 0; j < count; j++) {
                    automaton.getNextTransition(t);
                    if (automaton.isAccept(t.dest) && anyTermID >= t.min && anyTermID <= t.max) {
                        throw new IllegalStateException("automaton cannot end with an ANY transition");
                    }
                }
            }

            int termCount = termToID.size();

            // We have to carefully translate these transitions so automaton
            // realizes they also match all other terms:
            Automaton newAutomaton = new Automaton();
            for (int i = 0; i < numStates; i++) {
                newAutomaton.createState();
                newAutomaton.setAccept(i, automaton.isAccept(i));
            }

            for (int i = 0; i < numStates; i++) {
                count = automaton.initTransition(i, t);
                for (int j = 0; j < count; j++) {
                    automaton.getNextTransition(t);
                    int min, max;
                    if (t.min <= anyTermID && anyTermID <= t.max) {
                        // Match any term
                        min = 0;
                        max = termCount - 1;
                    } else {
                        min = t.min;
                        max = t.max;
                    }
                    newAutomaton.addTransition(t.source, t.dest, min, max);
                }
            }
            newAutomaton.finishState();
            automaton = newAutomaton;
        }

        det = Operations.removeDeadStates(Operations.determinize(automaton, maxDeterminizedStates));
    }

    private int getTermID(BytesRef term) {
        Integer id = termToID.get(term);
        if (id == null) {
            id = termToID.size();
            if (term != null) {
                term = BytesRef.deepCopyOf(term);
            }
            termToID.put(term, id);
            idToTerm.put(id, term);
            if (term == null) {
                anyTermID = id;
            }
        }

        return id;
    }
}
