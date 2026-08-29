/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringColumnValues;
import org.elasticsearch.columnar.string.Vocabulary;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;
import java.util.Random;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * What a dictionary would cover on each benchmark shape as the budget it is allowed grows, which is what
 * {@link DictionaryPolicy}'s bounds are chosen against. Prints the curve, and holds the survey to what those
 * bounds assume of it: that a larger budget never covers less, and that a column of few repeated terms is
 * named in full while one of nearly unique values is not named at all.
 */
public class DictionaryPolicyCurveTests extends ESTestCase {

    private static final int DOCS = 2_000_000;
    private static final int[] BUDGETS_KB = { 256, 512, 1024, 2048, 4096, 8192 };

    public void testCoverageAgainstBudget() throws IOException {
        System.out.println(
            String.format(Locale.ROOT, "%-20s %9s %7s %8s %9s %9s %s", "shape", "budgetKB", "terms", "dictKB", "coverage", "share", "kept?")
        );
        for (StringData data : StringData.values()) {
            final BytesRef[] values = data.generate(DOCS, new Random(7));
            double leastBudgetCoverage = -1;
            double mostBudgetCoverage = -1;
            for (int kb : BUDGETS_KB) {
                // Only the budget constrains the survey here; whether to keep what it found is asked after.
                final DictionaryPolicy surveying = new DictionaryPolicy(kb * 1024, 0.0, 1.0);
                final Vocabulary.Terms terms = Vocabulary.survey(cursor(values), surveying, values.length);
                if (terms == null) {
                    // Nothing worth naming: every value is nearly its own term.
                    System.out.println(String.format(Locale.ROOT, "%-20s %9d %7s", data, kb, "none"));
                    continue;
                }
                if (leastBudgetCoverage < 0) {
                    leastBudgetCoverage = terms.coverage();
                }
                mostBudgetCoverage = terms.coverage();
                final double share = (double) terms.dictionaryBytes() / terms.columnBytes();
                // The bounds as they ship, against what a dictionary of this size would have covered.
                final boolean kept = terms.coverage() >= 0.5 && share <= 0.2 && terms.dictionaryBytes() <= 512 * 1024;
                System.out.println(
                    String.format(
                        Locale.ROOT,
                        "%-20s %9d %7d %8.1f %9.3f %9.3f %s",
                        data,
                        kb,
                        terms.size(),
                        terms.dictionaryBytes() / 1024.0,
                        terms.coverage(),
                        share,
                        kept ? "yes" : ""
                    )
                );
            }
            if (leastBudgetCoverage >= 0) {
                assertThat(
                    data + " covered less of the column when allowed more room for terms",
                    mostBudgetCoverage,
                    greaterThanOrEqualTo(leastBudgetCoverage)
                );
            }
        }
    }

    /** A column repeating a handful of terms is named in full well inside the bounds as they ship. */
    public void testFewRepeatedTermsAreNamedInFull() throws IOException {
        for (StringData data : new StringData[] { StringData.LOG_LEVEL, StringData.HIT_COLOR, StringData.HOSTNAME }) {
            final Vocabulary.Terms terms = Vocabulary.survey(
                cursor(data.generate(DOCS, new Random(7))),
                ColumNARDocValuesFormat.DEFAULT_DICTIONARY_POLICY,
                DOCS
            );
            assertNotNull(data + " surveyed no terms", terms);
            assertEquals(data + " left values unnamed", 1.0, terms.coverage(), 0.0);
        }
    }

    /** A column of nearly unique values is not worth naming however much room the terms are given. */
    public void testNearlyUniqueValuesAreNotNamed() throws IOException {
        for (StringData data : new StringData[] { StringData.TRACE_ID, StringData.URL }) {
            final BytesRef[] values = data.generate(DOCS, new Random(7));
            for (int kb : BUDGETS_KB) {
                final Vocabulary.Terms terms = Vocabulary.survey(cursor(values), new DictionaryPolicy(kb * 1024, 0.0, 1.0), DOCS);
                assertNull(data + " named terms at " + kb + "KB", terms);
            }
        }
    }

    private static StringColumnValues cursor(BytesRef[] values) {
        return new StringColumnValues() {
            private int doc = -1;

            @Override
            public int valueCount() {
                return values[doc] == null ? 0 : 1;
            }

            @Override
            public void nextValue() {}

            @Override
            public BytesRef value() {
                return values[doc];
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) {
                for (doc = target; doc < values.length; doc++) {
                    if (values[doc] != null) {
                        return doc;
                    }
                }
                return doc = DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return values.length;
            }
        };
    }
}
