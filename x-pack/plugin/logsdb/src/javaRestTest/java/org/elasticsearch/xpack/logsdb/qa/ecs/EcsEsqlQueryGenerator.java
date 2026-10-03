/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa.ecs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Generates random ES|QL queries over the ECS field catalog defined by
 * {@link EcsLogsDataGenerator#fields()}. Every generated query is totally ordered —
 * the result set can be compared row-for-row between the two index modes without sorting.
 *
 * <p>Queries use an {@code $index} placeholder that the caller replaces with the actual
 * data stream name before execution.
 *
 * <p>Commands deliberately excluded, each with a comment:
 * <ul>
 *   <li>{@code TOP} – tie-breaking is arbitrary when values are equal; not deterministic.</li>
 *   <li>{@code MEDIAN} / {@code PERCENTILE} – TDigest merge order is index-mode-dependent.</li>
 *   <li>{@code SAMPLE} – non-deterministic by design.</li>
 *   <li>{@code METADATA _score} – score depends on segment structure, not field values.</li>
 *   <li>Unsorted {@code LIMIT} – a LIMIT without a preceding total-order SORT is arbitrary.</li>
 * </ul>
 *
 * <p>{@code COUNT_DISTINCT} and {@code VALUES} are restricted to
 * {@linkplain EcsLogsDataGenerator.Field#lowCardinality() low-cardinality} fields. With a
 * corpus of 20,000–100,000 documents, applying either to a high-cardinality field (e.g.
 * {@code log_id} or {@code @timestamp}) would exceed the default 3000 precision threshold
 * causing approximate results, or produce a result set too large to compare efficiently.
 *
 * <p>{@code SORT} is never applied to {@code text}, {@code ip}, or multi-valued fields because
 * ES|QL does not support sorting on those types.
 *
 * <p>Keyword equality, {@code IN}, and {@code LIKE} predicates are only generated for fields
 * that have an entry in {@link #KEYWORD_POOLS}, so every literal in a generated predicate is
 * provably present in the corpus and the query is non-vacuous. Similarly, numeric thresholds
 * are drawn from {@link #NUMERIC_THRESHOLDS} (covering each field's actual value range),
 * date ranges are derived from the corpus timestamps, and CIDR ranges are chosen from
 * {@link #CIDRS} which only contains prefixes that match at least one pooled IP value.
 *
 * <p>{@code IS NULL} is only generated for fields that are not always present — see
 * {@link EcsLogsDataGenerator.Field#alwaysPresent()} — so it always has a chance of matching.
 */
public class EcsEsqlQueryGenerator {

    /**
     * Keyword value pools keyed by field name. Only fields with an entry here are used in
     * keyword equality, IN, and LIKE predicates — this guarantees every generated literal exists
     * in the corpus. Fields without an entry are still reachable via KEEP, SORT, IS NOT NULL,
     * EVAL, and STATS.
     */
    private static final Map<String, String[]> KEYWORD_POOLS = Map.ofEntries(
        Map.entry("log.level", EcsLogsDataGenerator.LOG_LEVELS),
        Map.entry("http.request.method", EcsLogsDataGenerator.HTTP_METHODS),
        Map.entry("event.outcome", EcsLogsDataGenerator.EVENT_OUTCOMES),
        Map.entry("event.action", EcsLogsDataGenerator.EVENT_ACTIONS),
        Map.entry("host.name", EcsLogsDataGenerator.HOST_NAMES),
        Map.entry("host.architecture", EcsLogsDataGenerator.HOST_ARCHS),
        Map.entry("service.name", EcsLogsDataGenerator.SERVICE_NAMES),
        Map.entry("service.version", EcsLogsDataGenerator.SERVICE_VERSIONS),
        Map.entry("service.environment", EcsLogsDataGenerator.SERVICE_ENVS),
        Map.entry("cloud.provider", EcsLogsDataGenerator.CLOUD_PROVIDERS),
        Map.entry("cloud.region", EcsLogsDataGenerator.CLOUD_REGIONS),
        Map.entry("cloud.availability_zone", EcsLogsDataGenerator.CLOUD_AZS),
        Map.entry("url.domain", EcsLogsDataGenerator.URL_DOMAINS),
        Map.entry("url.path", EcsLogsDataGenerator.URL_PATHS),
        Map.entry("user_agent.original", EcsLogsDataGenerator.USER_AGENTS),
        Map.entry("error.type", EcsLogsDataGenerator.ERROR_TYPES),
        Map.entry("error.code", EcsLogsDataGenerator.ERROR_CODES),
        Map.entry("container.name", EcsLogsDataGenerator.CONTAINER_NAMES)
    );

    /**
     * MATCH search terms per text field. Drawn from each field's own value pool so the term is
     * provably present in the standard-analyzer output. All pool strings consist of ASCII letters
     * and spaces, so the standard tokeniser produces exactly the listed tokens.
     *
     * <p>{@code error.message} is a flavor-1-only field: {@code appendDocument} only emits it
     * when {@code ordinal % 3 == 1}. For those ordinals, {@code ordinal % 6 ∈ {1, 4}}, so only
     * {@code ERROR_MESSAGES[1]} ("Null pointer encountered") and
     * {@code ERROR_MESSAGES[4]} ("Service unavailable") are ever indexed. Terms from the other four
     * ERROR_MESSAGES entries (refused, timed, invalid, …) are never present in the corpus and must
     * not be used here.
     *
     * <p>{@code user_agent.original.text} is a flavor-0-only field. The standard tokeniser splits
     * on {@code /} and {@code .}, so e.g. "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0" produces
     * tokens including "mozilla", "chrome", "linux". Every term listed here is present in at least
     * one {@link EcsLogsDataGenerator#USER_AGENTS} entry.
     */
    private static final Map<String, String[]> MATCH_TERMS = Map.of(
        "message",
        new String[] { "processed", "authenticated", "established", "connection", "request" },
        "error.message",
        new String[] { "null", "pointer", "encountered", "service", "unavailable" },
        "user_agent.original.text",
        new String[] { "mozilla", "chrome", "firefox", "curl", "python" }
    );

    /**
     * CIDR ranges per ip field. Each CIDR matches at least one value in that field's address pool,
     * so {@code CIDR_MATCH} is never vacuously empty.
     */
    private static final Map<String, String[]> CIDRS = Map.of(
        "host.ip",
        new String[] { "10.0.0.0/8", "192.168.0.0/16", "172.16.0.0/12" },
        "client.ip",
        new String[] { "203.0.113.0/24", "198.51.100.0/24", "192.0.2.0/24", "10.10.0.0/16" }
    );

    /**
     * Numeric thresholds per field: values that produce non-trivial selectivity for {@code >} and
     * {@code <=} across the generated value range. There is no {@code default} branch — calling
     * this method with a field not in the map is a generator bug.
     */
    private static final Map<String, long[]> NUMERIC_THRESHOLDS = Map.of(
        "event.duration",
        new long[] { 1_000_000L, 5_000_000L, 50_000_000L },
        "event.risk_score",
        new long[] { 10L, 50L, 90L },
        "http.request.bytes",
        new long[] { 512L, 1_024L, 4_096L },
        "http.response.status_code",
        new long[] { 200L, 300L, 400L, 500L },
        "http.response.bytes",
        new long[] { 1_024L, 8_192L, 32_768L },
        "process.pid",
        new long[] { 1_000L, 10_000L, 50_000L }
    );

    private final Random random;
    private final int corpusSize;
    private final List<EcsLogsDataGenerator.Field> all;
    private final List<EcsLogsDataGenerator.Field> keywords;
    private final List<EcsLogsDataGenerator.Field> numerics;
    private final List<EcsLogsDataGenerator.Field> dates;
    private final List<EcsLogsDataGenerator.Field> ips;
    private final List<EcsLogsDataGenerator.Field> texts;
    private final List<EcsLogsDataGenerator.Field> sortable;
    private final List<EcsLogsDataGenerator.Field> groupable;
    private final List<EcsLogsDataGenerator.Field> lowCard;
    /** Keyword fields with a {@link #KEYWORD_POOLS} entry: literals provably exist in the corpus. */
    private final List<EcsLogsDataGenerator.Field> poolKeywords;
    /** Numeric fields with a {@link #NUMERIC_THRESHOLDS} entry. */
    private final List<EcsLogsDataGenerator.Field> poolNumerics;
    /** Fields that are not always present: {@code IS NULL} has a chance of matching. */
    private final List<EcsLogsDataGenerator.Field> nullableFields;
    /**
     * Always-present pool-keyword fields. Safe as {@code AND} operands: two flavor-agnostic
     * fields AND'd together can never produce an empty result by construction, unlike e.g.
     * an HTTP-only field AND'd with an error-only field.
     */
    private final List<EcsLogsDataGenerator.Field> compoundKeywords;

    public EcsEsqlQueryGenerator(Random random, int corpusSize) {
        this.random = random;
        this.corpusSize = corpusSize;
        this.all = EcsLogsDataGenerator.fields();
        this.keywords = filter(all, "keyword", false);
        this.numerics = filterTypes(all, "long", "double");
        this.dates = filter(all, "date", false);
        this.ips = filter(all, "ip", false);
        this.texts = filterTypes(all, "text");
        this.sortable = all.stream().filter(EcsLogsDataGenerator.Field::sortable).collect(Collectors.toList());
        // groupable: sortable, not text, not multi-valued (GROUP BY on multi-valued fans out rows)
        this.groupable = sortable.stream()
            .filter(f -> "text".equals(f.esqlType()) == false)
            .filter(f -> f.multiValued() == false)
            .collect(Collectors.toList());
        this.lowCard = all.stream().filter(EcsLogsDataGenerator.Field::lowCardinality).collect(Collectors.toList());
        this.poolKeywords = keywords.stream().filter(f -> KEYWORD_POOLS.containsKey(f.name())).collect(Collectors.toList());
        this.poolNumerics = numerics.stream().filter(f -> NUMERIC_THRESHOLDS.containsKey(f.name())).collect(Collectors.toList());
        this.nullableFields = all.stream().filter(f -> f.alwaysPresent() == false).collect(Collectors.toList());
        this.compoundKeywords = poolKeywords.stream().filter(EcsLogsDataGenerator.Field::alwaysPresent).collect(Collectors.toList());
    }

    // ── public API ────────────────────────────────────────────────────────────────────────────

    /** Generates a random filter query (WHERE + row output). */
    public String randomFilterQuery() {
        StringBuilder q = new StringBuilder();
        q.append("FROM $index");
        addWhereClauses(q, randomIntBetween(0, 2));
        addKeepAndSort(q);
        return q.toString();
    }

    /** Generates a random STATS aggregation query. */
    public String randomStatsQuery() {
        StringBuilder q = new StringBuilder();
        q.append("FROM $index");
        // WHERE is optional before STATS
        if (random.nextInt(3) == 0) {
            addWhereClauses(q, 1);
        }
        addStats(q);
        return q.toString();
    }

    /** Generates a random EVAL query. */
    public String randomEvalQuery() {
        StringBuilder q = new StringBuilder();
        q.append("FROM $index");
        if (random.nextInt(2) == 0) {
            addWhereClauses(q, 1);
        }
        addEval(q);
        addKeepAndSort(q);
        return q.toString();
    }

    // ── pipeline stages ───────────────────────────────────────────────────────────────────────

    private void addWhereClauses(StringBuilder q, int n) {
        if (n == 0) {
            return;
        }
        // The first WHERE clause can be any predicate (may reference flavor-specific fields).
        q.append("\n  | WHERE ").append(randomPredicate());
        // Additional WHERE clauses are equivalent to AND with the first clause. Using a second
        // randomPredicate() call risks combining predicates from different document flavors
        // (e.g. HTTP-only AND error-only), producing an empty intersection by construction.
        // simpleCompoundPredicate() is restricted to always-present fields so it can never
        // eliminate rows that survived the first clause due to a flavor mismatch.
        for (int i = 1; i < n; i++) {
            q.append("\n  | WHERE ").append(simpleCompoundPredicate());
        }
    }

    private void addKeepAndSort(StringBuilder q) {
        // Always include log_id so we have a unique tiebreaker
        List<String> cols = new ArrayList<>();
        cols.add("log_id");
        int extra = randomIntBetween(1, 3);
        List<EcsLogsDataGenerator.Field> candidates = new ArrayList<>(all);
        candidates.removeIf(f -> "text".equals(f.esqlType())); // KEEP text is fine but SORT isn't
        shuffle(candidates);
        for (int i = 0; i < extra && i < candidates.size(); i++) {
            String name = candidates.get(i).name();
            if (cols.contains(name) == false) {
                cols.add(name);
            }
        }
        q.append("\n  | KEEP ").append(String.join(", ", cols));

        // SORT: 0-2 sortable columns (not text, not ip, not multi-valued) then log_id as tiebreaker
        List<String> sortCols = new ArrayList<>();
        List<EcsLogsDataGenerator.Field> sortCandidates = new ArrayList<>(sortable);
        sortCandidates.removeIf(f -> f.name().equals("log_id"));
        shuffle(sortCandidates);
        int nSort = randomIntBetween(0, Math.min(2, sortCandidates.size()));
        for (int i = 0; i < nSort; i++) {
            String col = sortCandidates.get(i).name();
            if (cols.contains(col)) {
                sortCols.add(col + (random.nextBoolean() ? " ASC" : " DESC") + " NULLS LAST");
            }
        }
        sortCols.add("log_id ASC");
        q.append("\n  | SORT ").append(String.join(", ", sortCols));
        q.append("\n  | LIMIT ").append(randomLimit());
    }

    private void addStats(StringBuilder q) {
        // 1-3 aggregations
        int nAggs = randomIntBetween(1, 3);
        List<String> aggs = new ArrayList<>();
        for (int i = 0; i < nAggs; i++) {
            aggs.add(randomAgg("agg" + i));
        }
        q.append("\n  | STATS ").append(String.join(", ", aggs));

        // 0-2 grouping fields
        int nGroups = randomIntBetween(0, 2);
        List<String> groupCols = new ArrayList<>();
        List<EcsLogsDataGenerator.Field> groupCandidates = new ArrayList<>(groupable);
        shuffle(groupCandidates);
        for (int i = 0; i < nGroups && i < groupCandidates.size(); i++) {
            groupCols.add(groupCandidates.get(i).name());
        }
        if (groupCols.isEmpty() == false) {
            q.append(" BY ").append(String.join(", ", groupCols));
        }

        // Sort all group keys for a total order (COUNT(*) is not a reliable tiebreaker)
        if (groupCols.isEmpty() == false) {
            List<String> sortParts = groupCols.stream().map(c -> c + " ASC NULLS LAST").collect(Collectors.toList());
            q.append("\n  | SORT ").append(String.join(", ", sortParts));
        }
        q.append("\n  | LIMIT ").append(randomLimit());
    }

    private void addEval(StringBuilder q) {
        // 1-2 simple EVAL expressions
        int n = randomIntBetween(1, 2);
        List<String> exprs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            exprs.add(randomEvalExpr("e" + i));
        }
        q.append("\n  | EVAL ").append(String.join(", ", exprs));
    }

    // ── predicate generation ──────────────────────────────────────────────────────────────────

    private String randomPredicate() {
        int choice = random.nextInt(11);
        switch (choice) {
            case 0 -> {
                // Keyword equality: pick from fields with a pool so the literal provably exists.
                EcsLogsDataGenerator.Field f = pick(poolKeywords);
                return f.name() + " == \"" + randomKeywordValue(f) + "\"";
            }
            case 1 -> {
                // Keyword IN list: both values drawn from the field's pool.
                EcsLogsDataGenerator.Field f = pick(poolKeywords);
                String v1 = randomKeywordValue(f);
                String v2 = randomKeywordValue(f);
                return f.name() + " IN (\"" + v1 + "\", \"" + v2 + "\")";
            }
            case 2 -> {
                // Keyword LIKE prefix: the first two characters of a pooled value are used as the
                // prefix, so the pattern provably matches at least that one value. No fallthrough
                // for high-cardinality fields — every pool field can produce a valid prefix.
                EcsLogsDataGenerator.Field f = pick(poolKeywords);
                String val = randomKeywordValue(f);
                String prefix = val.length() > 2 ? val.substring(0, 2) : val;
                return f.name() + " LIKE \"" + prefix + "*\"";
            }
            case 3 -> {
                // IS NULL: pick a field that is sometimes absent so the predicate can actually match.
                // IS NOT NULL: any field, since always-present fields return TRUE for every row too.
                if (random.nextBoolean()) {
                    EcsLogsDataGenerator.Field f = pick(nullableFields);
                    return f.name() + " IS NULL";
                } else {
                    EcsLogsDataGenerator.Field f = pick(all);
                    return f.name() + " IS NOT NULL";
                }
            }
            case 4 -> {
                // Numeric comparison: threshold drawn from the field's actual value range.
                EcsLogsDataGenerator.Field f = pick(poolNumerics);
                long threshold = randomNumericThreshold(f);
                String op = random.nextBoolean() ? ">" : "<=";
                return f.name() + " " + op + " " + threshold;
            }
            case 5 -> {
                // Date range on @timestamp derived from the corpus size. Sampling two ordinals
                // produces a window of genuinely random width — unlike a hardcoded range that
                // matches 100% of a small corpus or 0% of a large one.
                int o1 = random.nextInt(corpusSize);
                int o2 = random.nextInt(corpusSize);
                int lo = Math.min(o1, o2);
                int hi = Math.max(o1, o2);
                // Ensure a non-empty window: if both samples are the same ordinal, open a 1-second gap.
                if (lo == hi) {
                    if (hi < corpusSize - 1) {
                        hi++;
                    } else {
                        lo--;
                    }
                }
                return "@timestamp >= \""
                    + EcsLogsDataGenerator.timestampAt(lo)
                    + "\" AND @timestamp < \""
                    + EcsLogsDataGenerator.timestampAt(hi)
                    + "\"";
            }
            case 6 -> {
                // CIDR_MATCH: ranges chosen from each field's own address space.
                EcsLogsDataGenerator.Field f = pick(ips);
                String cidr = pick(CIDRS.get(f.name()));
                return "CIDR_MATCH(" + f.name() + ", \"" + cidr + "\")";
            }
            case 7 -> {
                // MATCH on text: term drawn from the field's own message pool so it is provably
                // present in the standard-analyzer output.
                EcsLogsDataGenerator.Field f = pick(texts);
                String term = pick(MATCH_TERMS.get(f.name()));
                return "MATCH(" + f.name() + ", \"" + term + "\")";
            }
            case 8 -> {
                // AND: restrict both operands to always-present fields so the conjunction cannot
                // be vacuous due to document-flavor gating (e.g. HTTP-only AND error-only).
                return "(" + simpleCompoundPredicate() + " AND " + simpleCompoundPredicate() + ")";
            }
            case 9 -> {
                // OR: widening — any simple predicate is safe as an OR operand.
                return "(" + simplePredicate() + " OR " + simplePredicate() + ")";
            }
            case 10 -> {
                // Keyword not-equal: eliminates one value from the match set; matches all rows
                // where the field holds a different pool value.
                EcsLogsDataGenerator.Field f = pick(poolKeywords);
                return f.name() + " != \"" + randomKeywordValue(f) + "\"";
            }
            default -> throw new AssertionError("unexpected choice: " + choice);
        }
    }

    /**
     * Simple predicate (no nesting) used as an {@code OR} operand. May reference any field.
     */
    private String simplePredicate() {
        int choice = random.nextInt(4);
        return switch (choice) {
            case 0 -> {
                EcsLogsDataGenerator.Field f = pick(poolKeywords);
                yield f.name() + " == \"" + randomKeywordValue(f) + "\"";
            }
            case 1 -> pick(all).name() + " IS NOT NULL";
            case 2 -> {
                EcsLogsDataGenerator.Field f = pick(poolNumerics);
                yield f.name() + " > " + randomNumericThreshold(f);
            }
            case 3 -> {
                // Lower bound on @timestamp: always matches the suffix of the corpus after this ordinal.
                int o = random.nextInt(corpusSize);
                yield "@timestamp >= \"" + EcsLogsDataGenerator.timestampAt(o) + "\"";
            }
            default -> throw new AssertionError("unexpected choice: " + choice);
        };
    }

    /**
     * Simple predicate restricted to always-present, pool-backed keyword fields. Used as an
     * {@code AND} operand so that two independently-selective predicates cannot produce an empty
     * conjunction due to document-flavor gating.
     *
     * <p>Uses {@code IN (v1, v2)} rather than equality so that the predicate covers at least two
     * distinct modular residues of the corpus ordinal, which dramatically reduces the chance of a
     * structurally-empty conjunction when the first {@code WHERE} clause has already narrowed the
     * ordinal to a specific residue class (e.g. {@code log.level LIKE "DE*"} implicitly selects
     * {@code ordinal % 5 == 0}).
     *
     * <p>{@code @timestamp} lower bounds are intentionally excluded: if the first clause already
     * contains an upper-bound timestamp filter, adding an independent lower bound can produce an
     * impossible conjunction ({@code @timestamp >= T2 AND @timestamp < T1} where {@code T2 > T1}).
     */
    private String simpleCompoundPredicate() {
        int choice = random.nextInt(3);
        return switch (choice) {
            case 0 -> {
                // IN (v1, v2) on an always-present field: covers two ordinal residues so the
                // conjunction with a narrowing first-clause predicate is rarely vacuous.
                EcsLogsDataGenerator.Field f = pick(compoundKeywords);
                String v1 = randomKeywordValue(f);
                String v2 = randomKeywordValue(f);
                yield f.name() + " IN (\"" + v1 + "\", \"" + v2 + "\")";
            }
            case 1 -> {
                // IS NOT NULL on an always-present field: always TRUE, so this operand is a no-op
                // that leaves whatever the first clause selected fully intact.
                EcsLogsDataGenerator.Field f = pick(compoundKeywords);
                yield f.name() + " IS NOT NULL";
            }
            case 2 -> {
                // != on an always-present field: eliminates one pool value from the match set,
                // so the conjunction with the first clause is rarely vacuous.
                EcsLogsDataGenerator.Field f = pick(compoundKeywords);
                yield f.name() + " != \"" + randomKeywordValue(f) + "\"";
            }
            default -> throw new AssertionError("unexpected choice: " + choice);
        };
    }

    // ── aggregation generation ────────────────────────────────────────────────────────────────

    private String randomAgg(String alias) {
        int choice = random.nextInt(7);
        return switch (choice) {
            case 0 -> alias + " = COUNT(*)";
            case 1 -> {
                EcsLogsDataGenerator.Field f = pick(numerics);
                yield alias + " = " + pick(new String[] { "MIN", "MAX", "SUM", "AVG" }) + "(" + f.name() + ")";
            }
            case 2 -> {
                EcsLogsDataGenerator.Field f = pick(numerics);
                yield alias + " = COUNT(" + f.name() + ")";
            }
            case 3 -> {
                // COUNT_DISTINCT only on low-cardinality fields to stay below 3000 threshold
                EcsLogsDataGenerator.Field f = pick(lowCard);
                yield alias + " = COUNT_DISTINCT(" + f.name() + ")";
            }
            case 4 -> {
                // VALUES: low-cardinality, non-multi-valued keyword only
                List<EcsLogsDataGenerator.Field> valCandidates = lowCard.stream()
                    .filter(f -> "keyword".equals(f.esqlType()))
                    .filter(f -> f.multiValued() == false)
                    .collect(Collectors.toList());
                EcsLogsDataGenerator.Field f = pick(valCandidates);
                yield alias + " = MV_SORT(VALUES(" + f.name() + "))";
            }
            case 5 -> alias + " = COUNT(*)";
            case 6 -> {
                EcsLogsDataGenerator.Field f = pick(numerics);
                yield alias + " = MAX(" + f.name() + ")";
            }
            default -> throw new AssertionError("unexpected choice: " + choice);
        };
    }

    // ── EVAL expression generation ────────────────────────────────────────────────────────────

    private String randomEvalExpr(String alias) {
        int choice = random.nextInt(4);
        return switch (choice) {
            case 1 -> {
                EcsLogsDataGenerator.Field f = pick(numerics);
                yield alias + " = " + f.name() + " * 2";
            }
            case 2 -> {
                EcsLogsDataGenerator.Field f = pick(keywords);
                yield alias + " = TO_UPPER(" + f.name() + ")";
            }
            case 3 -> {
                EcsLogsDataGenerator.Field f = pick(keywords);
                yield alias + " = CONCAT(" + f.name() + ", \"_suffix\")";
            }
            case 0 -> {
                EcsLogsDataGenerator.Field f = pick(dates);
                yield alias + " = DATE_TRUNC(1 hour, " + f.name() + ")";
            }
            default -> throw new AssertionError("unexpected choice: " + choice);
        };
    }

    // ── value sampling from pools ─────────────────────────────────────────────────────────────

    /**
     * Returns a random value from this field's keyword pool. Every returned value is actually
     * indexed in the corpus, so any generated predicate using this value is non-vacuous.
     * Calling this with a field that has no {@link #KEYWORD_POOLS} entry is a generator bug.
     */
    private String randomKeywordValue(EcsLogsDataGenerator.Field f) {
        return pick(KEYWORD_POOLS.get(f.name()));
    }

    /**
     * Returns a random numeric threshold for this field drawn from {@link #NUMERIC_THRESHOLDS}.
     * The threshold covers the field's generated value range and produces non-trivial selectivity
     * for both {@code >} and {@code <=}. Calling this with a field not in the map is a generator bug.
     */
    private long randomNumericThreshold(EcsLogsDataGenerator.Field f) {
        return pick(NUMERIC_THRESHOLDS.get(f.name()));
    }

    // ── helpers ───────────────────────────────────────────────────────────────────────────────

    private int randomLimit() {
        // Small limits: comparing 100k rows twice per query is unnecessary overhead.
        // The total-order SORT guarantees the prefix is a meaningful comparison.
        return random.nextBoolean() ? randomIntBetween(1, 50) : randomIntBetween(51, 200);
    }

    private int randomIntBetween(int min, int max) {
        return min + random.nextInt(max - min + 1);
    }

    private <T> T pick(List<T> list) {
        return list.get(random.nextInt(list.size()));
    }

    private <T> T pick(T[] arr) {
        return arr[random.nextInt(arr.length)];
    }

    private long pick(long[] arr) {
        return arr[random.nextInt(arr.length)];
    }

    private <T> void shuffle(List<T> list) {
        for (int i = list.size() - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            T tmp = list.get(i);
            list.set(i, list.get(j));
            list.set(j, tmp);
        }
    }

    private static List<EcsLogsDataGenerator.Field> filter(List<EcsLogsDataGenerator.Field> fields, String type, boolean multiValued) {
        return fields.stream().filter(f -> type.equals(f.esqlType()) && f.multiValued() == multiValued).collect(Collectors.toList());
    }

    private static List<EcsLogsDataGenerator.Field> filterTypes(List<EcsLogsDataGenerator.Field> fields, String... types) {
        var typeSet = java.util.Set.of(types);
        return fields.stream().filter(f -> typeSet.contains(f.esqlType())).collect(Collectors.toList());
    }
}
