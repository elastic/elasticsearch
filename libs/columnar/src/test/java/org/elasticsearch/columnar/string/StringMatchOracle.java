/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Predicate;

final class StringMatchOracle {

    private StringMatchOracle() {}

    static boolean inRange(BytesRef value, BytesRef lower, boolean includeLower, BytesRef upper, boolean includeUpper) {
        if (lower != null) {
            final int cmp = value.compareTo(lower);
            if (cmp < 0 || (cmp == 0 && includeLower == false)) {
                return false;
            }
        }
        if (upper != null) {
            final int cmp = value.compareTo(upper);
            return cmp < 0 || (cmp == 0 && includeUpper);
        }
        return true;
    }

    static boolean startsWith(BytesRef value, BytesRef prefix) {
        if (value.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (value.bytes[value.offset + i] != prefix.bytes[prefix.offset + i]) {
                return false;
            }
        }
        return true;
    }

    static List<Integer> expected(BytesRef[][] docSlots, Predicate<BytesRef> matches) {
        final List<Integer> docs = new ArrayList<>();
        for (int doc = 0; doc < docSlots.length; doc++) {
            if (docSlots[doc] == null) {
                continue;
            }
            for (BytesRef value : docSlots[doc]) {
                if (value != null && matches.test(value)) {
                    docs.add(doc);
                    break;
                }
            }
        }
        return docs;
    }

    static List<Integer> matched(DocIdSetIterator iterator) throws IOException {
        final List<Integer> docs = new ArrayList<>();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }

    static List<Integer> matchedInWindows(DocIdSetIterator iterator, int docCount) throws IOException {
        final TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(iterator);
        if (twoPhase == null) {
            return matched(iterator);
        }
        final int window = ESTestCase.randomFrom(1, 7, 64, 512, 5000, docCount + 1);
        final FixedBitSet bits = new FixedBitSet(docCount);
        final DocIdSetIterator approximation = twoPhase.approximation();
        approximation.nextDoc();
        for (int upTo = window; approximation.docID() != DocIdSetIterator.NO_MORE_DOCS; upTo += window) {
            twoPhase.intoBitSet(Math.min(upTo, docCount), bits, 0);
            if (upTo >= docCount) {
                break;
            }
        }
        final List<Integer> docs = new ArrayList<>();
        for (int doc = bits.nextSetBit(0); doc != DocIdSetIterator.NO_MORE_DOCS; doc = doc + 1 < bits.length()
            ? bits.nextSetBit(doc + 1)
            : DocIdSetIterator.NO_MORE_DOCS) {
            docs.add(doc);
        }
        return docs;
    }

    static void checkRandomQueries(StringColumnReader reader, BytesRef[][] docSlots, BytesRef[] pool, int queries) throws IOException {
        for (int i = 0; i < queries; i++) {
            switch (ESTestCase.between(0, 4)) {
                case 0 -> {
                    final BytesRef term = boundFrom(pool, false);
                    assertMatches(docSlots, term::equals, () -> reader.matchTerm(term));
                }
                case 1 -> {
                    final BytesRef lower = boundFrom(pool, true);
                    final BytesRef upper = boundFrom(pool, true);
                    assertRange(reader, docSlots, lower, ESTestCase.randomBoolean(), upper, ESTestCase.randomBoolean());
                }
                case 2 -> {
                    final BytesRef value = ESTestCase.randomBoolean() ? ESTestCase.randomFrom(pool) : PlainSkipTestValues.randomValue();
                    final BytesRef prefix = new BytesRef(value.bytes, value.offset, ESTestCase.between(0, value.length));
                    assertMatches(docSlots, candidate -> StringMatchOracle.startsWith(candidate, prefix), () -> reader.matchPrefix(prefix));
                }
                case 3 -> {
                    final NavigableSet<BytesRef> terms = new TreeSet<>();
                    for (int t = ESTestCase.between(1, 5); t > 0; t--) {
                        terms.add(boundFrom(pool, false));
                    }
                    assertMatches(docSlots, terms::contains, () -> reader.matchAnyOf(terms, terms));
                }
                case 4 -> assertRange(reader, docSlots, null, true, null, true);
                default -> throw new AssertionError(i);
            }
        }
    }

    static void assertRange(
        StringColumnReader reader,
        BytesRef[][] docSlots,
        BytesRef lower,
        boolean includeLower,
        BytesRef upper,
        boolean includeUpper
    ) throws IOException {
        assertMatches(
            docSlots,
            value -> StringMatchOracle.inRange(value, lower, includeLower, upper, includeUpper),
            () -> reader.matchRange(lower, includeLower, upper, includeUpper)
        );
    }

    static void assertMatches(BytesRef[][] docSlots, Predicate<BytesRef> matches, IOSupplier<DocIdSetIterator> iterator)
        throws IOException {
        final List<Integer> expected = StringMatchOracle.expected(docSlots, matches);
        Assert.assertEquals(expected, StringMatchOracle.matched(iterator.get()));
        Assert.assertEquals(expected, StringMatchOracle.matchedInWindows(iterator.get(), docSlots.length));
    }

    static BytesRef boundFrom(BytesRef[] pool, boolean nullable) {
        return switch (ESTestCase.between(0, 4)) {
            case 0 -> nullable ? null : ESTestCase.randomFrom(pool);
            case 1, 2 -> ESTestCase.randomFrom(pool);
            case 3 -> PlainSkipTestValues.randomValue();
            case 4 -> {
                final BytesRef base = ESTestCase.randomFrom(pool);
                final byte[] bytes = new byte[base.length + 1];
                System.arraycopy(base.bytes, base.offset, bytes, 0, base.length);
                yield new BytesRef(bytes);
            }
            default -> throw new AssertionError();
        };
    }
}
