/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefComparator;
import org.apache.lucene.util.StringSorter;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A query for matching any value from a set of BytesRef values for a specific field.
 * <p>
 * This implementation is slow, because it potentially scans binary doc values for each document.
 */
public final class SlowCustomBinaryDocValuesTermInSetQuery extends AbstractBinaryDocValuesQuery {

    private final PrefixCodedTerms termData;
    private final int termDataHashCode;

    public SlowCustomBinaryDocValuesTermInSetQuery(String fieldName, List<BytesRef> terms) {
        this(fieldName, packTerms(fieldName, Objects.requireNonNull(terms)));
    }

    private SlowCustomBinaryDocValuesTermInSetQuery(String fieldName, PrefixCodedTerms termData) {
        super(fieldName, buildMatchPredicate(termData));
        this.termData = termData;
        this.termDataHashCode = termData.hashCode();
    }

    private static Predicate<BytesRef> buildMatchPredicate(PrefixCodedTerms termData) {
        try {
            var automaton = Automata.makeBinaryStringUnion(termData.iterator());
            var runAutomaton = new ByteRunAutomaton(automaton, true);
            return term -> runAutomaton.run(term.bytes, term.offset, term.length);
        } catch (java.io.IOException e) {
            // Shouldn't happen since termData.iterator() provides an iterator implementation that never throws
            assert false : e;
            throw new UncheckedIOException(e);
        }
    }

    private static PrefixCodedTerms packTerms(String field, List<BytesRef> terms) {
        BytesRef[] sortedTerms = terms.toArray(new BytesRef[0]);
        new StringSorter(BytesRefComparator.NATURAL) {

            @Override
            protected void get(BytesRefBuilder builder, BytesRef result, int i) {
                BytesRef term = sortedTerms[i];
                result.length = term.length;
                result.offset = term.offset;
                result.bytes = term.bytes;
            }

            @Override
            protected void swap(int i, int j) {
                BytesRef tmp = sortedTerms[i];
                sortedTerms[i] = sortedTerms[j];
                sortedTerms[j] = tmp;
            }
        }.sort(0, sortedTerms.length);
        PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
        BytesRefBuilder previous = null;
        for (BytesRef term : sortedTerms) {
            if (previous == null) {
                previous = new BytesRefBuilder();
            } else if (previous.get().equals(term)) {
                continue; // deduplicate
            }
            builder.add(field, term);
            previous.copyBytes(term);
        }
        return builder.finish();
    }

    @Override
    protected float matchCost() {
        // the cost for single term matching is 10, ByteRunAutomaton.run() is O(term length)
        // so this is only slightly slower than single term
        return 11;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder("SlowCustomBinaryDocValuesTermInSetQuery(fieldName=");
        sb.append(field).append(",terms=[");
        PrefixCodedTerms.TermIterator iterator = termData.iterator();
        boolean first = true;
        for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
            if (first == false) {
                sb.append(",");
            }
            sb.append(term.utf8ToString());
            first = false;
        }
        sb.append("])");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        SlowCustomBinaryDocValuesTermInSetQuery that = (SlowCustomBinaryDocValuesTermInSetQuery) o;
        return Objects.equals(fieldName, that.fieldName) && termDataHashCode == that.termDataHashCode && termData.equals(that.termData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, termDataHashCode);
    }
}
