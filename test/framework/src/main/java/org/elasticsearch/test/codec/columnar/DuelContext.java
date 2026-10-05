/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.codec.columnar;

import org.elasticsearch.client.internal.Client;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * Everything a {@link BehaviorCheck} needs to run one baseline-versus-contender comparison: the client, both
 * index names and configs, the field names, the scenario, the write plan, and the corpus. It also derives
 * expected values (oracles) directly from the corpus so a check can validate the baseline response before
 * comparing it to the contender, which keeps broken setup from masquerading as a contender difference. Most
 * oracles model the invariant view every layout agrees on: sorted, deduplicated, non-null values. The
 * value-multiplicity oracles count every non-null value occurrence, including intra-document duplicates, which
 * both strict columnar layouts keep.
 *
 * @param duelName       the name of the duel pair, included in failure messages
 * @param client         the client to query through
 * @param baselineIndex  the baseline index name
 * @param contenderIndex the contender (ColumNAR) index name
 * @param baselineConfig the baseline layout
 * @param contenderConfig the contender layout
 * @param keywordField   the keyword field under test
 * @param docIdField     the numeric identity field used as the sort tiebreak and retrieval anchor
 * @param scenario       the corpus scenario
 * @param writePlan      the write and merge plan applied to both indices
 * @param docs           the corpus indexed into both indices
 */
public record DuelContext(
    String duelName,
    Client client,
    String baselineIndex,
    String contenderIndex,
    KeywordIndexConfig baselineConfig,
    KeywordIndexConfig contenderConfig,
    String keywordField,
    String docIdField,
    KeywordScenario scenario,
    BehaviorWritePlan writePlan,
    List<KeywordDoc> docs
) {

    /**
     * @param checkName the check reporting the failure
     * @return a prefix naming the check, scenario, both layouts, and the write plan. The assertion adds its own
     *         comparison name to the message.
     */
    public String failureContext(final String checkName) {
        return "duel=["
            + duelName
            + "] check=["
            + checkName
            + "] scenario=["
            + scenario.name()
            + "] baseline=["
            + baselineConfig.layoutLabel()
            + "] contender=["
            + contenderConfig.layoutLabel()
            + "] writePlan=["
            + writePlan
            + "] field=["
            + keywordField
            + "]";
    }

    /**
     * @return the sorted, distinct, non-null values across the whole corpus.
     */
    public List<String> distinctValues() {
        final Set<String> distinct = new TreeSet<>();
        for (final KeywordDoc doc : docs) {
            distinct.addAll(doc.sortedDistinctValues());
        }
        return new ArrayList<>(distinct);
    }

    /**
     * @param limit the maximum number of terms to return
     * @return up to {@code limit} values that appear in the corpus, chosen deterministically.
     */
    public List<String> presentTerms(int limit) {
        final List<String> distinct = distinctValues();
        return distinct.subList(0, Math.min(limit, distinct.size()));
    }

    /**
     * @return a value that does not appear in the corpus.
     */
    public String absentTerm() {
        final Set<String> distinct = new TreeSet<>(distinctValues());
        String candidate = "__absent__";
        while (distinct.contains(candidate)) {
            candidate = candidate + "_";
        }
        return candidate;
    }

    /**
     * @param term a keyword value
     * @return the doc ids whose values contain {@code term}.
     */
    public Set<Long> docIdsContaining(final String term) {
        final Set<Long> matches = new TreeSet<>();
        for (final KeywordDoc doc : docs) {
            if (doc.sortedDistinctValues().contains(term)) {
                matches.add(doc.docId());
            }
        }
        return matches;
    }

    /**
     * @return the doc ids that have at least one non-null value.
     */
    public Set<Long> docIdsWithAnyValue() {
        final Set<Long> matches = new TreeSet<>();
        for (final KeywordDoc doc : docs) {
            if (doc.sortedDistinctValues().isEmpty() == false) {
                matches.add(doc.docId());
            }
        }
        return matches;
    }

    /**
     * @return the doc ids that hold no value (the {@code IS NULL} oracle).
     */
    public Set<Long> docIdsWithoutValue() {
        final Set<Long> matches = new TreeSet<>();
        for (final KeywordDoc doc : docs) {
            if (doc.sortedDistinctValues().isEmpty()) {
                matches.add(doc.docId());
            }
        }
        return matches;
    }

    /**
     * @param predicate a test over a single keyword value
     * @return the doc ids that hold at least one value satisfying {@code predicate}. This is the shared oracle
     *         for the value-matching query surfaces (prefix, wildcard, regexp, range, and their ES|QL
     *         equivalents): matching is value-based, so it is invariant to order and duplicates.
     */
    public Set<Long> docIdsWithValueMatching(final Predicate<String> predicate) {
        final Set<Long> matches = new TreeSet<>();
        for (final KeywordDoc doc : docs) {
            if (doc.sortedDistinctValues().stream().anyMatch(predicate)) {
                matches.add(doc.docId());
            }
        }
        return matches;
    }

    /**
     * @return a corpus value made only of ASCII letters and digits, safe to use verbatim as a query-parser term
     *         without escaping or tokenization. Falls back to {@link #absentTerm()} when the scenario has no such
     *         value, which keeps the derived check deterministic (it then matches nothing on both sides).
     */
    public String literalTerm() {
        for (final String value : distinctValues()) {
            if (value.matches("[a-zA-Z0-9]+")) {
                return value;
            }
        }
        return absentTerm();
    }

    /**
     * @return a single ASCII letter or digit taken from the first character of a present value that starts with
     *         one, safe to use as a prefix, wildcard, or regexp pattern fragment without escaping. Unlike
     *         {@link #literalTerm()} it needs only a safe first character, so it still exercises corpora whose
     *         values embed punctuation, such as high cardinality. Falls back to {@link #absentTerm()}'s first
     *         character when no present value starts with an ASCII letter or digit.
     */
    public String prefixCharacter() {
        for (final String value : distinctValues()) {
            if (value.isEmpty() == false && isAsciiLetterOrDigit(value.charAt(0))) {
                return value.substring(0, 1);
            }
        }
        return absentTerm().substring(0, 1);
    }

    private static boolean isAsciiLetterOrDigit(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
    }

    /**
     * @return the sorted distinct ASCII-only values. Range bounds are taken from these so lexicographic order is
     *         unambiguous (ASCII bytes sort identically under UTF-8, UTF-16, and ES|QL keyword comparison), and
     *         any non-ASCII value sorts above every ASCII bound in every one of those orders.
     */
    public List<String> asciiValues() {
        return distinctValues().stream().filter(value -> value.chars().allMatch(codeUnit -> codeUnit < 0x80)).toList();
    }

    /**
     * @return the number of documents containing each value (the terms-aggregation oracle).
     */
    public Map<String, Long> valueDocCounts() {
        final Map<String, Long> counts = new LinkedHashMap<>();
        for (final KeywordDoc doc : docs) {
            for (final String value : doc.sortedDistinctValues()) {
                counts.merge(value, 1L, Long::sum);
            }
        }
        return counts;
    }

    /**
     * @return the expected {@code value_count}: the total number of non-null value occurrences across the corpus,
     *         including intra-document duplicates, which both strict columnar layouts keep.
     */
    public long expectedValueCount() {
        long total = 0;
        for (final KeywordDoc doc : docs) {
            total += doc.nonNullValues().size();
        }
        return total;
    }

    /**
     * @return the expected per-value counts for a composite terms source, which iterates doc-values entries:
     *         every occurrence including intra-document duplicates, which both strict columnar layouts keep.
     */
    public Map<String, Long> expectedValueBuckets() {
        final Map<String, Long> counts = new LinkedHashMap<>();
        for (final KeywordDoc doc : docs) {
            for (final String value : doc.nonNullValues()) {
                counts.merge(value, 1L, Long::sum);
            }
        }
        return counts;
    }

    /**
     * @return each document's sorted, deduplicated values keyed by doc id (the doc-values retrieval oracle).
     */
    public Map<Long, List<String>> perDocSortedDistinct() {
        final Map<Long, List<String>> byDoc = new TreeMap<>();
        for (final KeywordDoc doc : docs) {
            byDoc.put(doc.docId(), doc.sortedDistinctValues());
        }
        return byDoc;
    }

    /**
     * @return each document's values in source order, keyed by doc id, keeping duplicates and inline nulls in
     *         place, with an absent field mapped to an empty list (the {@code _source} round-trip oracle: the
     *         document-order array the columnar {@code ArrayOrderInlineNull} layout reconstructs). Inline nulls
     *         are preserved because Elasticsearch keeps them in the reconstructed {@code _source} array.
     */
    public Map<Long, List<String>> perDocOrderedValues() {
        final Map<Long, List<String>> byDoc = new TreeMap<>();
        for (final KeywordDoc doc : docs) {
            byDoc.put(doc.docId(), doc.values() == null ? List.of() : doc.values());
        }
        return byDoc;
    }
}
