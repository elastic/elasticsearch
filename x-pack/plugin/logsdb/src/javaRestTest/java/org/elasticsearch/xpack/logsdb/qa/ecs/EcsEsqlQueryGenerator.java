/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa.ecs;

import java.util.ArrayList;
import java.util.List;
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
 * corpus of 100k-300k documents, applying either to a high-cardinality field (e.g. {@code log_id}
 * or {@code @timestamp}) would exceed the default 3000 precision threshold causing approximate
 * results, or produce a result set too large to compare efficiently.
 *
 * <p>{@code SORT} is never applied to {@code text}, {@code ip}, or multi-valued fields because
 * ES|QL does not support sorting on those types.
 */
public class EcsEsqlQueryGenerator {

    private final Random random;
    private final List<EcsLogsDataGenerator.Field> all;
    private final List<EcsLogsDataGenerator.Field> keywords;
    private final List<EcsLogsDataGenerator.Field> numerics;
    private final List<EcsLogsDataGenerator.Field> dates;
    private final List<EcsLogsDataGenerator.Field> ips;
    private final List<EcsLogsDataGenerator.Field> texts;
    private final List<EcsLogsDataGenerator.Field> sortable;
    private final List<EcsLogsDataGenerator.Field> groupable;
    private final List<EcsLogsDataGenerator.Field> lowCard;

    public EcsEsqlQueryGenerator(Random random) {
        this.random = random;
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
        for (int i = 0; i < n; i++) {
            q.append("\n  | WHERE ").append(randomPredicate());
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
        int choice = random.nextInt(10);
        switch (choice) {
            case 0 -> {
                // keyword equality
                EcsLogsDataGenerator.Field f = pick(keywords);
                return f.name() + " == \"" + randomKeywordValue(f) + "\"";
            }
            case 1 -> {
                // keyword IN list
                EcsLogsDataGenerator.Field f = pick(keywords);
                String v1 = randomKeywordValue(f);
                String v2 = randomKeywordValue(f);
                return f.name() + " IN (\"" + v1 + "\", \"" + v2 + "\")";
            }
            case 2 -> {
                // keyword LIKE prefix
                EcsLogsDataGenerator.Field f = pick(keywords);
                if (f.lowCardinality()) {
                    String val = randomKeywordValue(f);
                    String prefix = val.length() > 2 ? val.substring(0, 2) : val;
                    return f.name() + " LIKE \"" + prefix + "*\"";
                }
                return pick(keywords).name() + " IS NOT NULL";
            }
            case 3 -> {
                // IS NULL / IS NOT NULL
                EcsLogsDataGenerator.Field f = pick(all);
                return f.name() + (random.nextBoolean() ? " IS NULL" : " IS NOT NULL");
            }
            case 4 -> {
                // numeric comparison
                EcsLogsDataGenerator.Field f = pick(numerics);
                long threshold = randomNumericThreshold(f);
                String op = random.nextBoolean() ? ">" : "<=";
                return f.name() + " " + op + " " + threshold;
            }
            case 5 -> {
                // date range
                EcsLogsDataGenerator.Field f = pick(dates);
                return f.name() + " >= \"2024-01-01T00:00:00.000Z\" AND " + f.name() + " < \"2024-01-08T00:00:00.000Z\"";
            }
            case 6 -> {
                // CIDR_MATCH on ip
                EcsLogsDataGenerator.Field f = pick(ips);
                String cidr = pick(new String[] { "10.0.0.0/8", "192.168.0.0/16", "203.0.113.0/24" });
                return "CIDR_MATCH(" + f.name() + ", \"" + cidr + "\")";
            }
            case 7 -> {
                // MATCH on text
                EcsLogsDataGenerator.Field f = pick(texts);
                String term = pick(new String[] { "processed", "authenticated", "established", "failed", "refused" });
                return "MATCH(" + f.name() + ", \"" + term + "\")";
            }
            case 8 -> {
                // AND
                return "(" + simplePredicate() + " AND " + simplePredicate() + ")";
            }
            case 9 -> {
                // OR
                return "(" + simplePredicate() + " OR " + simplePredicate() + ")";
            }
            default -> throw new AssertionError("unexpected choice: " + choice);
        }
    }

    /** Simpler predicate (no AND/OR nesting) used in compound predicates. */
    private String simplePredicate() {
        int choice = random.nextInt(4);
        return switch (choice) {
            case 0 -> {
                EcsLogsDataGenerator.Field f = pick(keywords);
                yield f.name() + " == \"" + randomKeywordValue(f) + "\"";
            }
            case 1 -> pick(all).name() + " IS NOT NULL";
            case 2 -> {
                EcsLogsDataGenerator.Field f = pick(numerics);
                yield f.name() + " > " + randomNumericThreshold(f);
            }
            case 3 -> {
                EcsLogsDataGenerator.Field f = pick(dates);
                yield f.name() + " >= \"2024-01-01T00:00:00.000Z\"";
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

    private String randomKeywordValue(EcsLogsDataGenerator.Field f) {
        return switch (f.name()) {
            case "log.level" -> pick(EcsLogsDataGenerator.LOG_LEVELS);
            case "http.request.method" -> pick(EcsLogsDataGenerator.HTTP_METHODS);
            case "event.outcome" -> pick(EcsLogsDataGenerator.EVENT_OUTCOMES);
            case "event.action" -> pick(EcsLogsDataGenerator.EVENT_ACTIONS);
            case "host.name" -> pick(EcsLogsDataGenerator.HOST_NAMES);
            case "service.name" -> pick(EcsLogsDataGenerator.SERVICE_NAMES);
            case "service.environment" -> pick(EcsLogsDataGenerator.SERVICE_ENVS);
            case "cloud.provider" -> pick(EcsLogsDataGenerator.CLOUD_PROVIDERS);
            case "cloud.region" -> pick(EcsLogsDataGenerator.CLOUD_REGIONS);
            case "cloud.availability_zone" -> pick(EcsLogsDataGenerator.CLOUD_AZS);
            case "url.domain" -> pick(EcsLogsDataGenerator.URL_DOMAINS);
            case "url.path" -> pick(EcsLogsDataGenerator.URL_PATHS);
            case "error.type" -> pick(EcsLogsDataGenerator.ERROR_TYPES);
            case "error.code" -> pick(EcsLogsDataGenerator.ERROR_CODES);
            case "container.name" -> pick(EcsLogsDataGenerator.CONTAINER_NAMES);
            case "tags" -> pick(new String[] { "critical", "production", "audit", "deprecated", "experimental" });
            // default: fields with no dedicated value pool (log.logger, service.version, etc.)
            // return a generic string that is always present in the index as a dynamic value
            default -> "example";
        };
    }

    private long randomNumericThreshold(EcsLogsDataGenerator.Field f) {
        return switch (f.name()) {
            case "http.response.status_code" -> pick(new long[] { 200L, 300L, 400L, 500L });
            case "http.request.bytes", "http.response.bytes" -> pick(new long[] { 512L, 1024L, 4096L });
            case "event.duration" -> pick(new long[] { 1_000_000L, 10_000_000L, 100_000_000L });
            case "process.pid" -> pick(new long[] { 1000L, 10000L, 50000L });
            case "event.risk_score" -> pick(new long[] { 10L, 50L, 90L });
            // default: numeric fields with no specific pool — threshold 0 filters ~half the corpus
            default -> 0L;
        };
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
