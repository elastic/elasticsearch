/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import org.elasticsearch.index.query.IntervalQueryBuilder;
import org.elasticsearch.index.query.IntervalsSourceProvider;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.codec.columnar.BehaviorCheck;
import org.elasticsearch.test.codec.columnar.DuelContext;
import org.elasticsearch.test.codec.columnar.DuelDoc;
import org.elasticsearch.test.codec.columnar.TextValues;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import static org.elasticsearch.index.codec.columnar.DslRequests.docIds;
import static org.elasticsearch.index.codec.columnar.DslRequests.sorted;
import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertEquals;
import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertSameElements;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * DSL checks for {@code text} and {@code match_only_text} fields. A text field is searched by the tokens of its
 * analyzed value whether or not it indexes terms, so the same checks run with the default {@code index} and with
 * {@code index: false}. The oracles come from {@link TextValues#tokens}, which models the analysis independently of
 * the code under test.
 */
public final class DslTextChecks {

    private static final int PREFIX_LENGTH = 2;

    private DslTextChecks() {}

    /**
     * @return the token, scored match, phrase, and phrase prefix checks.
     */
    public static List<BehaviorCheck> checks() {
        return List.of(new TokenQueryCheck(), new ScoredMatchCheck(), new PhraseCheck(), new PhrasePrefixCheck());
    }

    /**
     * @return the {@code intervals} check, which a {@code match_only_text} field mapped with {@code index: false}
     *         does not answer.
     */
    public static BehaviorCheck intervals() {
        return new IntervalsCheck();
    }

    private record Probe(String label, QueryBuilder query, Set<Long> expected, boolean mustMatch) {}

    private abstract static class ProbeCheck implements BehaviorCheck {
        private final String name;

        ProbeCheck(final String name) {
            this.name = name;
        }

        abstract List<Probe> probes(DuelContext ctx);

        @Override
        public final String name() {
            return name;
        }

        @Override
        public final void check(final DuelContext ctx) {
            for (final Probe probe : probes(ctx)) {
                final String context = ctx.failureContext(name + "[" + probe.label() + "]");
                if (probe.mustMatch() && probe.expected().isEmpty()) {
                    throw new AssertionError(context + " stage=[probe] the oracle matches no document, so the probe would compare nothing");
                }
                final List<Long> expected = sorted(probe.expected());
                final List<Long> baseline = sorted(docIds(ctx.client(), ctx.baselineIndex(), probe.query(), ctx.docIdField()));
                final List<Long> contender = sorted(docIds(ctx.client(), ctx.contenderIndex(), probe.query(), ctx.docIdField()));
                assertSameElements(context + " stage=[baseline-oracle]", expected, baseline);
                assertSameElements(context + " stage=[contender-vs-baseline]", baseline, contender);
            }
        }
    }

    static final class TokenQueryCheck extends ProbeCheck {
        TokenQueryCheck() {
            super("dsl_text_token_queries");
        }

        @Override
        List<Probe> probes(final DuelContext ctx) {
            final List<String> pair = adjacentPair(ctx);
            final String token = pair.get(0);
            final String prefix = prefixOf(token);
            return List.of(
                new Probe("term", QueryBuilders.termQuery(ctx.field(), token), docIdsWhere(ctx, tokens -> tokens.contains(token)), true),
                new Probe("term_absent", QueryBuilders.termQuery(ctx.field(), ctx.absentTerm()), Set.of(), false),
                new Probe(
                    "terms",
                    QueryBuilders.termsQuery(ctx.field(), pair),
                    docIdsWhere(ctx, tokens -> tokens.contains(pair.get(0)) || tokens.contains(pair.get(1))),
                    true
                ),
                new Probe(
                    "prefix",
                    QueryBuilders.prefixQuery(ctx.field(), prefix),
                    docIdsWhere(ctx, tokens -> tokens.stream().anyMatch(t -> t.startsWith(prefix))),
                    true
                ),
                new Probe(
                    "wildcard",
                    QueryBuilders.wildcardQuery(ctx.field(), prefix + "*"),
                    docIdsWhere(ctx, tokens -> tokens.stream().anyMatch(t -> t.startsWith(prefix))),
                    true
                ),
                new Probe(
                    "regexp",
                    QueryBuilders.regexpQuery(ctx.field(), prefix + ".*"),
                    docIdsWhere(ctx, tokens -> tokens.stream().anyMatch(t -> t.startsWith(prefix))),
                    true
                )
            );
        }
    }

    static final class ScoredMatchCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_scored_match";
        }

        @Override
        public void check(final DuelContext ctx) {
            final List<String> pair = adjacentPair(ctx);
            for (final List<String> tokens : List.of(List.of(pair.get(0)), pair)) {
                final String text = String.join(" ", tokens);
                final QueryBuilder query = QueryBuilders.matchQuery(ctx.field(), text);
                final Set<Long> expected = docIdsWhere(ctx, valueTokens -> tokens.stream().anyMatch(valueTokens::contains));
                final String context = ctx.failureContext(name() + "[" + text + "]");
                if (expected.isEmpty()) {
                    throw new AssertionError(context + " stage=[probe] the oracle matches no document, so the probe would compare nothing");
                }
                final List<String> baseline = scoredHits(ctx, ctx.baselineIndex(), query);
                final List<String> contender = scoredHits(ctx, ctx.contenderIndex(), query);
                assertSameElements(context + " stage=[baseline-oracle]", sorted(expected), docIdsOf(baseline));
                assertEquals(context + " stage=[contender-vs-baseline]", baseline, contender);
            }
        }

        private static List<String> scoredHits(final DuelContext ctx, final String index, final QueryBuilder query) {
            final List<String> hits = new ArrayList<>();
            assertResponse(
                ctx.client()
                    .prepareSearch(index)
                    .setQuery(query)
                    .setSize(DslRequests.MAX_HITS)
                    .addSort(SortBuilders.scoreSort())
                    .addSort(SortBuilders.fieldSort(ctx.docIdField()).order(SortOrder.ASC))
                    .addDocValueField(ctx.docIdField()),
                response -> {
                    for (final SearchHit hit : response.getHits().getHits()) {
                        hits.add(((Number) hit.field(ctx.docIdField()).getValue()).longValue() + "|" + hit.getScore());
                    }
                }
            );
            return hits;
        }

        private static List<Long> docIdsOf(final List<String> scoredHits) {
            return scoredHits.stream().map(hit -> Long.parseLong(hit.substring(0, hit.indexOf('|')))).toList();
        }
    }

    static final class PhraseCheck extends ProbeCheck {
        PhraseCheck() {
            super("dsl_match_phrase");
        }

        @Override
        List<Probe> probes(final DuelContext ctx) {
            final List<String> pair = adjacentPair(ctx);
            final List<Probe> probes = new ArrayList<>();
            probes.add(phraseProbe(ctx, "adjacent", pair, true));
            probes.add(phraseProbe(ctx, "reversed", List.of(pair.get(1), pair.get(0)), false));
            crossValuePair(ctx).ifPresent(cross -> probes.add(phraseProbe(ctx, "cross_value", cross, false)));
            return probes;
        }

        private static Probe phraseProbe(final DuelContext ctx, final String label, final List<String> phrase, boolean mustMatch) {
            return new Probe(
                label,
                QueryBuilders.matchPhraseQuery(ctx.field(), String.join(" ", phrase)),
                docIdsWhere(ctx, valueTokens -> containsSequence(valueTokens, phrase)),
                mustMatch
            );
        }
    }

    static final class PhrasePrefixCheck extends ProbeCheck {
        PhrasePrefixCheck() {
            super("dsl_match_phrase_prefix");
        }

        @Override
        List<Probe> probes(final DuelContext ctx) {
            final List<String> pair = adjacentPair(ctx);
            final List<Probe> probes = new ArrayList<>();
            probes.add(prefixProbe(ctx, "adjacent", pair, true));
            crossValuePair(ctx).ifPresent(cross -> probes.add(prefixProbe(ctx, "cross_value", cross, false)));
            return probes;
        }

        private static Probe prefixProbe(final DuelContext ctx, final String label, final List<String> pair, boolean mustMatch) {
            final String lead = pair.get(0);
            final String prefix = prefixOf(pair.get(1));
            return new Probe(
                label,
                QueryBuilders.matchPhrasePrefixQuery(ctx.field(), lead + " " + prefix),
                docIdsWhere(ctx, valueTokens -> containsLeadThenPrefix(valueTokens, lead, prefix)),
                mustMatch
            );
        }
    }

    static final class IntervalsCheck extends ProbeCheck {
        IntervalsCheck() {
            super("dsl_intervals");
        }

        @Override
        List<Probe> probes(final DuelContext ctx) {
            final List<String> pair = adjacentPair(ctx);
            final List<Probe> probes = new ArrayList<>();
            probes.add(intervalsProbe(ctx, "adjacent", pair, true));
            probes.add(intervalsProbe(ctx, "reversed", List.of(pair.get(1), pair.get(0)), false));
            crossValuePair(ctx).ifPresent(cross -> probes.add(intervalsProbe(ctx, "cross_value", cross, false)));
            return probes;
        }

        private static Probe intervalsProbe(final DuelContext ctx, final String label, final List<String> phrase, boolean mustMatch) {
            return new Probe(
                label,
                new IntervalQueryBuilder(
                    ctx.field(),
                    new IntervalsSourceProvider.Match(String.join(" ", phrase), 0, true, null, null, null)
                ),
                docIdsWhere(ctx, valueTokens -> containsSequence(valueTokens, phrase)),
                mustMatch
            );
        }
    }

    private static List<String> adjacentPair(final DuelContext ctx) {
        for (final DuelDoc doc : ctx.docs()) {
            for (final String value : doc.nonNullValues()) {
                final List<String> tokens = TextValues.tokens(value);
                if (tokens.size() >= 2) {
                    final int start = (tokens.size() - 2) / 2;
                    return List.of(tokens.get(start), tokens.get(start + 1));
                }
            }
        }
        throw new AssertionError(ctx.failureContext("probe") + " the corpus has no value with two tokens");
    }

    private static Optional<List<String>> crossValuePair(final DuelContext ctx) {
        for (final DuelDoc doc : ctx.docs()) {
            final List<String> values = doc.nonNullValues();
            for (int i = 0; i + 1 < values.size(); i++) {
                final List<String> first = TextValues.tokens(values.get(i));
                final List<String> second = TextValues.tokens(values.get(i + 1));
                if (first.isEmpty() == false && second.isEmpty() == false) {
                    return Optional.of(List.of(first.get(first.size() - 1), second.get(0)));
                }
            }
        }
        return Optional.empty();
    }

    private static Set<Long> docIdsWhere(final DuelContext ctx, final Predicate<List<String>> valueTokensMatch) {
        final Set<Long> ids = new TreeSet<>();
        for (final DuelDoc doc : ctx.docs()) {
            for (final String value : doc.nonNullValues()) {
                if (valueTokensMatch.test(TextValues.tokens(value))) {
                    ids.add(doc.docId());
                    break;
                }
            }
        }
        return ids;
    }

    private static boolean containsSequence(final List<String> tokens, final List<String> sequence) {
        return Collections.indexOfSubList(tokens, sequence) >= 0;
    }

    private static boolean containsLeadThenPrefix(final List<String> tokens, final String lead, final String prefix) {
        for (int i = 0; i + 1 < tokens.size(); i++) {
            if (tokens.get(i).equals(lead) && tokens.get(i + 1).startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static String prefixOf(final String token) {
        return token.substring(0, Math.min(PREFIX_LENGTH, token.length()));
    }
}
