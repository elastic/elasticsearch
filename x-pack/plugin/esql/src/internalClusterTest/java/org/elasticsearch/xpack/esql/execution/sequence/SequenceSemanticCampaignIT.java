/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.ql.plugin.QlPlugin;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Exercises seeded event histories through both real query engines and an independent full-history reference.
 * Ordinary test runs stay small; explicit campaign properties admit bounded chunks after a throughput pilot.
 * Each case checkpoints its input before indexing and preserves actual results when an assertion fails.
 * Generated ordering is tie-free per key; distinct same-timestamp events are outside this corpus.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceSemanticCampaignIT extends AbstractSequenceIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-11-11T00:00:00Z");
    private static final int MAX_BATCH_CASES = 2_000;
    private static final int MAX_CAMPAIGN_CASES = 20_000;
    private static final long MAX_CASE_BYTES = 8L * 1024 * 1024;
    private static final BigInteger BILLION = BigInteger.valueOf(1_000_000_000L);
    private static final TimeValue TIMEOUT = TimeValue.timeValueMinutes(2);
    private static final int MAX_DOCS = 5_000;
    private static final int[] BOUNDARIES = { 255, 256, 257, 511, 512, 513 };
    private static final long SEED_BASE = 0x51E0C0DE20261113L;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        assertTrue(plugins.removeIf(EsqlPlugin.class::isAssignableFrom));
        plugins.add(TrackingPlugin.class);
        plugins.add(EqlPlugin.class);
        plugins.add(QlPlugin.class);
        plugins.add(SecuritySettingPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int ordinal, Settings other) {
        return Settings.builder()
            .put(super.nodeSettings(ordinal, other))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "128mb")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(
                BreakerSettings.CIRCUIT_BREAKER_LIMIT_SETTING.getConcreteSettingForNamespace(EqlPlugin.CIRCUIT_BREAKER_NAME).getKey(),
                "128mb"
            )
            .build();
    }

    /** Supply security's setting while exercising the two real query actions without the security plugin. */
    public static class SecuritySettingPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(XPackSettings.SECURITY_ENABLED);
        }
    }

    /** Track all native compute blocks in the same way as SequenceEqlDifferentialIT. */
    public static class TrackingPlugin extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        protected BlockFactoryProvider blockFactoryProvider(BlockFactoryBuilder builder) {
            return new BlockFactoryProvider(new MockBlockFactory(builder));
        }
    }

    /**
     * Runs a small default corpus or an explicitly admitted, bounded campaign chunk. Full case evidence is written
     * in a checkpoint before indexing and completed in finally, independently of the random runner's state.
     */
    public void testGeneratedHistories() throws Exception {
        assertOracleContracts();
        int start = Integer.getInteger("tests.sequence.semantic.start", 0);
        int count = Integer.getInteger("tests.sequence.semantic.count", 54);
        assertTrue("invalid campaign chunk", start >= 0 && count >= 1 && count <= MAX_BATCH_CASES && start + count <= MAX_CAMPAIGN_CASES);
        if (count > 128) {
            assertEquals(
                "large chunks require explicit admission after the pilot",
                "true",
                System.getProperty("tests.sequence.semantic.campaign")
            );
        }
        boolean inject = Boolean.getBoolean("tests.sequence.semantic.inject_wrong_identity");
        assertTrue("sensitivity injection is a separate one-case run", inject == false || count == 1);
        int seconds = Integer.getInteger("tests.sequence.semantic.max_seconds", 900);
        assertTrue(seconds > 0 && seconds <= 21_600);
        String variant = System.getProperty("tests.sequence.semantic.variant", "ordinary");
        assertTrue(variant.matches("[A-Za-z0-9_-]+"));
        String explicitOutput = System.getProperty("tests.sequence.semantic.output");
        Path output = explicitOutput == null ? createTempDir("sequence-semantic") : Path.of(explicitOutput).toAbsolutePath();
        Files.createDirectories(output);
        try (var entries = Files.list(output)) {
            assertEquals("campaign output must be empty, preserving previous evidence", 0, entries.count());
        }
        logger.info("SEQUENCE semantic campaign output [{}], start [{}], count [{}]", output, start, count);
        write(
            output.resolve("configuration.json"),
            Map.of(
                "format",
                "sequence-semantic-v2",
                "variant",
                variant,
                "start",
                start,
                "count",
                count,
                "max_documents_per_case",
                MAX_DOCS,
                "max_case_bytes",
                MAX_CASE_BYTES,
                "max_wall_seconds",
                seconds,
                "eql_eligibility",
                "predeclared from fixture family; never adjusted after execution",
                "sensitivity_injection",
                inject
            )
        );
        long runStart = System.nanoTime();
        long deadline = runStart + TimeUnit.SECONDS.toNanos(seconds);
        var counts = new QueryCounts();
        Map<String, Long> coverage = new TreeMap<>();
        int completed = 0;
        long evidenceBytes = 0;
        Throwable failure = null;
        try {
            for (int number = start; number < start + count; number++) {
                assertTrue("campaign wall budget exhausted before completion", System.nanoTime() < deadline);
                Case fixture = generatedCase(number);
                Path casePath = output.resolve(fixture.name + ".json");
                try {
                    Map<String, Object> observation = runCase(fixture, casePath, counts, inject);
                    completed++;
                    evidenceBytes += Files.size(casePath);
                    addCoverage(coverage, fixture);
                    Files.writeString(
                        output.resolve("observations.ndjson"),
                        json(observation) + "\n",
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                    );
                } catch (SemanticMismatch mismatch) {
                    if (inject == false) {
                        try {
                            minimize(fixture, mismatch.lane, output, counts);
                        } catch (Exception | AssertionError minimizationFailure) {
                            mismatch.addSuppressed(minimizationFailure);
                        }
                    }
                    throw mismatch;
                }
            }
        } catch (Exception | AssertionError e) {
            failure = e;
            throw e;
        } finally {
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("variant", variant);
            summary.put("start", start);
            summary.put("requested_cases", count);
            summary.put("completed_cases", completed);
            summary.put("status", failure == null ? "passed" : "failed");
            summary.put("case_evidence_bytes", evidenceBytes);
            summary.put("runtime_wall_nanos", System.nanoTime() - runStart);
            summary.put("query_counts", counts.document());
            summary.put("coverage", coverage);
            summary.put("sensitivity_injection", inject);
            if (failure != null) {
                summary.put("failure", failure.toString());
            }
            try {
                write(output.resolve("summary.json"), summary);
            } catch (Exception | AssertionError evidenceFailure) {
                if (failure == null) {
                    throw evidenceFailure;
                }
                failure.addSuppressed(evidenceFailure);
            }
        }
    }

    /** A separate bounded profile checks incoming limits through real parsed queries without changing the legacy corpus. */
    public void testGeneratedPerLinkHistories() throws Exception {
        Map<String, Object> literalContracts = assertLinkOracleContracts();
        String explicitOutput = System.getProperty("tests.sequence.link.output");
        Path output = explicitOutput == null ? createTempDir("sequence-link-semantic") : Path.of(explicitOutput).toAbsolutePath();
        Files.createDirectories(output);
        try (var entries = Files.list(output)) {
            assertEquals("link evidence must start empty", 0, entries.count());
        }
        int seconds = Integer.getInteger("tests.sequence.link.max_seconds", 600);
        assertTrue(seconds > 0 && seconds <= 900);
        write(
            output.resolve("configuration.json"),
            Map.of(
                "format",
                "sequence-link-semantic-v1",
                "cases",
                96,
                "profiles",
                8,
                "max_wall_seconds",
                seconds,
                "expected_native_responses",
                288,
                "expected_eql_controls",
                8,
                "eql_eligibility",
                "predeclared two-stage absent/equal-overall controls only",
                "limits",
                "incoming from previous selected event; inclusive; overall maxspan also applies",
                "literal_contracts",
                literalContracts
            )
        );
        logger.info("SEQUENCE incoming-link semantic evidence [{}], cases [96]", output);
        long started = System.nanoTime();
        long deadline = started + TimeUnit.SECONDS.toNanos(seconds);
        QueryCounts counts = new QueryCounts();
        Map<String, Long> coverage = new TreeMap<>();
        int completed = 0;
        Throwable failure = null;
        try {
            for (int number = 0; number < 96; number++) {
                assertTrue("link profile wall budget exhausted", System.nanoTime() < deadline);
                LinkCase contract = generatedLinkCase(number);
                Case fixture = contract.fixture;
                Map<String, Object> input = fixtureDocument(fixture);
                input.put("incoming_max_gap_nanos", contract.incomingGaps.stream().map(Object::toString).toList());
                Path casePath = output.resolve(fixture.name + ".json");
                writeCase(
                    casePath,
                    Map.of(
                        "status",
                        "checking_literal_profile_count",
                        "input",
                        input,
                        "declared_expected_matches",
                        contract.expectedMatches
                    )
                );
                Files.writeString(
                    output.resolve("declared-contracts.ndjson"),
                    json(
                        Map.of(
                            "case",
                            fixture.name,
                            "profile",
                            fixture.profile,
                            "fixture_sha256",
                            sha(json(input)),
                            "declared_expected_matches",
                            contract.expectedMatches
                        )
                    ) + "\n",
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
                );
                assertEquals(
                    "the generated profile must retain its literal positive/negative outcome",
                    contract.expectedMatches,
                    historyOracle(fixture, contract.incomingGaps).size()
                );
                try {
                    Map<String, Object> observation = runCase(fixture, casePath, counts, false, contract.incomingGaps);
                    completed++;
                    addCoverage(coverage, fixture);
                    coverage.merge(contract.expectedMatches == 0 ? "negative_cases" : "positive_cases", 1L, Long::sum);
                    coverage.merge(
                        contract.incomingGaps.stream().skip(1).anyMatch(gap -> gap == 0) ? "has_absent_link" : "all_links_bounded",
                        1L,
                        Long::sum
                    );
                    Files.writeString(
                        output.resolve("observations.ndjson"),
                        json(observation) + "\n",
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                    );
                } catch (SemanticMismatch mismatch) {
                    try {
                        minimize(fixture, mismatch.lane, output, counts, contract.incomingGaps);
                    } catch (Exception | AssertionError minimizationFailure) {
                        mismatch.addSuppressed(minimizationFailure);
                    }
                    throw mismatch;
                }
            }
            assertEquals(96, completed);
            assertEquals(288, counts.nativeResponses);
            assertEquals(8, counts.eqlResponses);
        } catch (Exception | AssertionError problem) {
            failure = problem;
            throw problem;
        } finally {
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("format", "sequence-link-semantic-v1");
            summary.put("requested_cases", 96);
            summary.put("completed_cases", completed);
            summary.put("status", failure == null ? "passed" : "failed");
            summary.put("runtime_wall_nanos", System.nanoTime() - started);
            summary.put("query_counts", counts.document());
            summary.put("coverage", coverage);
            if (failure != null) summary.put("failure", failure.toString());
            try {
                write(output.resolve("summary.json"), summary);
            } catch (Exception | AssertionError evidenceFailure) {
                if (failure == null) throw evidenceFailure;
                failure.addSuppressed(evidenceFailure);
            }
        }
    }

    private Map<String, Object> runCase(Case fixture, Path output, QueryCounts counts, boolean inject) throws Exception {
        return runCase(fixture, output, counts, inject, List.of());
    }

    private Map<String, Object> runCase(Case fixture, Path output, QueryCounts counts, boolean inject, List<Long> incomingGaps)
        throws Exception {
        long started = System.nanoTime();
        assertTrue(fixture.events.size() <= MAX_DOCS);
        List<Selected> selected = historyOracle(fixture, incomingGaps);
        assertTrue("all expected matches must fit the explicit uncapped limit", selected.size() <= 10_000);
        Map<String, Object> expected = expectedResult(fixture, selected);
        String stagedQuery = incomingGaps.isEmpty() ? nativeQuery(fixture, false) : nativeQuery(fixture, false, incomingGaps);
        String outerQuery = incomingGaps.isEmpty() ? nativeQuery(fixture, true) : nativeQuery(fixture, true, incomingGaps);
        String countQuery = stagedQuery.substring(0, stagedQuery.lastIndexOf(" | SORT ")) + " | STATS matched = COUNT(*) | LIMIT 10000";
        Map<String, Object> input = fixtureDocument(fixture);
        String topology = topologySha(fixture);
        if (incomingGaps.isEmpty() == false) {
            input.put("incoming_max_gap_nanos", incomingGaps.stream().map(Object::toString).toList());
            topology = sha(json(Map.of("base_topology_sha256", topology, "incoming_max_gap_nanos", incomingGaps)));
        }
        var record = new LinkedHashMap<String, Object>();
        record.put("format", "sequence-semantic-case-v2");
        record.put("input", input);
        record.put("fixture_sha256", sha(json(input)));
        record.put("topology_sha256", topology);
        var queries = new LinkedHashMap<String, Object>();
        queries.put("stage_computation", stagedQuery);
        queries.put("outer_computation", outerQuery);
        queries.put("count", countQuery);
        if (fixture.eqlEligible) {
            queries.put(
                "eql",
                Map.of(
                    "query",
                    eqlQuery(fixture),
                    "tiebreaker_field",
                    "event.sequence",
                    "result_position",
                    "head",
                    "size",
                    10000,
                    "fetch_size",
                    256,
                    "allow_partial_search_results",
                    false,
                    "allow_partial_sequence_results",
                    false
                )
            );
        }
        record.put("queries", queries);
        record.put("oracle", expected);
        record.put("eql_eligible", fixture.eqlEligible);
        record.put("eql_reason", fixture.eqlReason);
        record.put("sensitivity_injection", inject);
        record.put("status", "running");
        writeCase(output, record);
        boolean created = false;
        Throwable failure = null;
        try {
            created = true;
            indexEvents(fixture);
            counts.nativeAttempts++;
            Map<String, Object> staged = nativeRun(stagedQuery, record, "stage_computation");
            counts.nativeResponses++;
            record.put("actual_stage_computation", staged);
            Map<String, Object> compared = inject ? corruptFirstIdentity(staged, fixture) : staged;
            if (inject) {
                record.put("actual_compared_after_explicit_injection", compared);
            }
            compare("stage_computation", expected, compared);
            assertReleased();

            counts.nativeAttempts++;
            Map<String, Object> outer = nativeRun(outerQuery, record, "outer_computation");
            counts.nativeResponses++;
            record.put("actual_outer_computation", outer);
            compare("outer_computation", expected, outer);
            compare("authoring_equivalence", staged, outer);
            assertReleased();

            counts.nativeAttempts++;
            Map<String, Object> actualCount = nativeRun(countQuery, record, "count");
            counts.nativeResponses++;
            record.put("actual_count", actualCount);
            compare("count", result(List.of(new Column("matched", "long")), List.of(List.of(Long.toString(selected.size())))), actualCount);
            assertReleased();

            if (fixture.eqlEligible) {
                counts.eqlAttempts++;
                Object eql = eqlRun(fixture);
                counts.eqlResponses++;
                record.put("actual_eql", eql);
                List<Object> expectedIdentities = selected.stream()
                    .map(s -> (Object) identityDocument(s))
                    .sorted(Comparator.comparing(SequenceSemanticCampaignIT::identitySortKey))
                    .toList();
                compare(
                    "eql_identities",
                    Map.of("sequences", expectedIdentities, "timeout", false, "partial", false, "shard_failures", List.of()),
                    eql
                );
                assertReleased();
            }
        } catch (Exception | AssertionError e) {
            failure = e;
        } finally {
            try {
                assertReleased();
            } catch (Exception | AssertionError e) {
                failure = combine(failure, e);
            }
            if (created) {
                try {
                    indicesAdmin().prepareDelete(fixture.indices().toArray(String[]::new))
                        .setIndicesOptions(org.elasticsearch.action.support.IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED)
                        .get();
                } catch (Exception | AssertionError e) {
                    failure = combine(failure, e);
                }
            }
            try {
                assertReleased();
            } catch (Exception | AssertionError e) {
                failure = combine(failure, e);
            }
            record.put("status", failure == null ? "passed" : "failed");
            record.put("case_wall_nanos", System.nanoTime() - started);
            if (failure != null) {
                record.put("failure", failure.toString());
                record.put("suppressed", java.util.Arrays.stream(failure.getSuppressed()).map(Object::toString).toList());
            }
            try {
                writeCase(output, record);
            } catch (Exception | AssertionError e) {
                failure = combine(failure, e);
            }
        }
        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure instanceof AssertionError assertion) {
            throw assertion;
        }
        return Map.of(
            "case",
            fixture.name,
            "seed",
            Long.toUnsignedString(fixture.seed),
            "profile",
            fixture.profile,
            "documents",
            fixture.events.size(),
            "matches",
            selected.size(),
            "eql_eligible",
            fixture.eqlEligible,
            "artifact_bytes",
            Files.size(output),
            "fixture_sha256",
            sha(json(input)),
            "topology_sha256",
            topology
        );
    }

    private static Throwable combine(Throwable first, Throwable next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }

    private static void compare(String lane, Object expected, Object actual) {
        if (Objects.equals(expected, actual) == false) {
            throw new SemanticMismatch(lane, expected, actual);
        }
    }

    private static Map<String, Object> corruptFirstIdentity(Map<String, Object> actual, Case fixture) {
        assertTrue(actual.get("rows") instanceof List<?>);
        List<?> sourceRows = (List<?>) actual.get("rows");
        assertFalse("the sensitivity case must contain a real match", sourceRows.isEmpty());
        List<List<Object>> rows = new ArrayList<>();
        for (Object row : sourceRows) {
            assertTrue(row instanceof List<?>);
            rows.add(new ArrayList<>((List<?>) row));
        }
        rows.getFirst().set(fixture.compound ? 3 : 2, "explicit-sensitivity-wrong-id");
        var changed = new LinkedHashMap<>(actual);
        changed.put("rows", rows);
        return changed;
    }

    /** Shrinks only a reproduced semantic lane, never an unrelated cleanup or infrastructure exception. */
    private void minimize(Case original, String lane, Path output, QueryCounts counts) throws Exception {
        minimize(original, lane, output, counts, List.of());
    }

    private void minimize(Case original, String lane, Path output, QueryCounts counts, List<Long> incomingGaps) throws Exception {
        long started = System.nanoTime();
        List<Event> kept = original.events;
        int attempts = 0;
        int accepted = 0;
        int granularity = 2;
        while (kept.size() > 1 && attempts < 32 && System.nanoTime() - started < TimeUnit.SECONDS.toNanos(120)) {
            int chunk = (kept.size() + granularity - 1) / granularity;
            boolean reduced = false;
            for (int first = 0; first < kept.size() && attempts < 32; first += chunk) {
                if (System.nanoTime() - started >= TimeUnit.SECONDS.toNanos(120)) {
                    break;
                }
                List<Event> candidate = new ArrayList<>(kept);
                candidate.subList(first, Math.min(first + chunk, kept.size())).clear();
                if (candidate.isEmpty()) {
                    continue;
                }
                Case smaller = original.withEvents(candidate);
                attempts++;
                try {
                    runCase(smaller, output.resolve("minimization-last-attempt.json"), counts, false, incomingGaps);
                } catch (SemanticMismatch mismatch) {
                    if (lane.equals(mismatch.lane)) {
                        kept = List.copyOf(candidate);
                        accepted++;
                        reduced = true;
                        granularity = 2;
                        Files.copy(
                            output.resolve("minimization-last-attempt.json"),
                            output.resolve("counterexample-minimized.json"),
                            java.nio.file.StandardCopyOption.REPLACE_EXISTING
                        );
                        break;
                    }
                }
            }
            if (reduced == false) {
                if (granularity >= kept.size()) {
                    break;
                }
                granularity = Math.min(kept.size(), granularity * 2);
            }
        }
        write(
            output.resolve("minimization.json"),
            Map.of(
                "case",
                original.name,
                "lane",
                lane,
                "original_documents",
                original.events.size(),
                "remaining_documents",
                kept.size(),
                "attempts",
                attempts,
                "accepted_reductions",
                accepted,
                "budget_seconds",
                120,
                "elapsed_nanos",
                System.nanoTime() - started,
                "status",
                "bounded delta debugging; not a global-minimum claim"
            )
        );
    }

    private static void addCoverage(Map<String, Long> coverage, Case fixture) {
        for (String key : List.of(
            "stages_" + fixture.stages,
            fixture.nanos ? "date_nanos" : "date",
            "branches_" + fixture.branches,
            fixture.compound ? "compound_key" : "scalar_key",
            fixture.expand ? "explicit_expansion" : "unexpanded",
            fixture.filtered ? "computed_filter" : "no_computed_filter",
            "filter_mode_" + fixture.filterMode,
            fixture.eqlEligible ? "eql_eligible" : "native_only",
            fixture.profile
        )) {
            coverage.merge(key, 1L, Long::sum);
        }
    }

    private static String topologySha(Case fixture) throws Exception {
        return sha(
            json(
                Map.of(
                    "stages",
                    fixture.stages,
                    "nanos",
                    fixture.nanos,
                    "branches",
                    fixture.branches,
                    "span_millis",
                    fixture.spanMillis,
                    "expand",
                    fixture.expand,
                    "compound",
                    fixture.compound,
                    "filtered",
                    fixture.filtered,
                    "filter_mode",
                    fixture.filterMode,
                    "events",
                    fixture.events.stream()
                        .sorted(Comparator.comparingLong((Event e) -> e.nanos).thenComparingInt(e -> e.ordinal))
                        .map(e -> List.of(e.branch, e.keys, e.bucket, e.mask, Long.toString(e.nanos), e.accepted))
                        .toList()
                )
            )
        );
    }

    private Map<String, Object> nativeRun(String query, Map<String, Object> evidence, String variant) throws Exception {
        String coordinator = internalCluster().getNodeNames()[0];
        ThreadContext context = internalCluster().getInstance(org.elasticsearch.threadpool.ThreadPool.class, coordinator)
            .getThreadContext();
        try (
            var ignored = context.stashContext();
            var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query)).actionGet(TIMEOUT)
        ) {
            var columns = response.columns().stream().map(c -> new Column(c.name(), c.outputType())).toList();
            List<List<Object>> rows = new ArrayList<>();
            Map<String, java.util.Set<String>> kinds = new TreeMap<>();
            for (Column column : columns)
                kinds.put(column.name, new java.util.TreeSet<>());
            for (List<Object> raw : getValuesList(response)) {
                List<Object> cells = new ArrayList<>();
                for (int i = 0; i < raw.size(); i++) {
                    cells.add(cell(columns.get(i).type, raw.get(i)));
                    collectKinds(raw.get(i), kinds.get(columns.get(i).name));
                }
                rows.add(cells);
            }
            Map<String, Object> kindDocument = new TreeMap<>();
            kinds.forEach((column, observed) -> kindDocument.put(column, List.copyOf(observed)));
            evidence.put(variant + "_observed_java_value_kinds", kindDocument);
            var result = new LinkedHashMap<>(result(columns, rows));
            result.put("partial", response.isPartial());
            result.put("running", response.isRunning());
            result.put("warnings", List.copyOf(context.getResponseHeaders().getOrDefault("Warning", List.of())));
            return result;
        }
    }

    private static String eqlQuery(Case fixture) {
        String key = fixture.compound ? "key, bucket" : "key";
        String predicates = java.util.stream.IntStream.range(0, fixture.stages)
            .mapToObj(i -> "[any where stage_" + (i + 1) + " == true]")
            .collect(Collectors.joining(" "));
        return "sequence by " + key + " with maxspan=" + fixture.spanMillis + "ms " + predicates;
    }

    private Map<String, Object> eqlRun(Case fixture) {
        var request = new EqlSearchRequest().indices(fixture.indices().toArray(String[]::new))
            .query(eqlQuery(fixture))
            .tiebreakerField("event.sequence")
            .resultPosition("head")
            .size(10000)
            .fetchSize(256)
            .allowPartialSearchResults(false)
            .allowPartialSequenceResults(false);
        var response = client().execute(EqlSearchAction.INSTANCE, request).actionGet(TIMEOUT);
        try {
            List<Object> sequences = new ArrayList<>();
            for (var match : response.hits().sequences()) {
                List<String> keys = match.joinKeys().stream().map(Object::toString).toList();
                sequences.add(
                    Map.of("keys", keys, "events", match.events().stream().map(event -> event.index() + "/" + event.id()).toList())
                );
            }
            sequences.sort(Comparator.comparing(SequenceSemanticCampaignIT::identitySortKey));
            return Map.of(
                "sequences",
                sequences,
                "timeout",
                response.isTimeout(),
                "partial",
                response.isPartial(),
                "shard_failures",
                java.util.Arrays.stream(response.shardFailures()).map(Object::toString).toList()
            );
        } finally {
            response.decRef();
        }
    }

    private void indexEvents(Case fixture) {
        for (String index : fixture.indices()) {
            assertAcked(indicesAdmin().prepareCreate(index).setSettings(indexSettings(1, 0)).setMapping(mapping(fixture)));
        }
        ensureGreen(fixture.indices().toArray(String[]::new));
        var bulk = client().prepareBulk();
        for (Event event : fixture.events) {
            bulk.add(prepareIndex(fixture.index(event)).setId(event.id).setRouting(event.route).setSource(source(fixture, event)));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(fixture.indices().toArray(String[]::new));
        var response = client().prepareSearch(fixture.indices().toArray(String[]::new)).setSize(0).setTrackTotalHits(true).get();
        try {
            assertEquals(
                "all immutable fixture events must be indexed once",
                fixture.events.size(),
                response.getHits().getTotalHits().value()
            );
        } finally {
            response.decRef();
        }
    }

    private static Map<String, Object> mapping(Case fixture) {
        Map<String, Object> properties = new TreeMap<>();
        for (String name : List.of("key", "labels", "event_uid", "route_key", "label", "tags", "raw_gate", "event.category")) {
            properties.put(name, Map.of("type", "keyword"));
        }
        for (String name : List.of("bucket", "n", "optional"))
            properties.put(name, Map.of("type", "integer"));
        properties.put("@timestamp", Map.of("type", fixture.nanos ? "date_nanos" : "date"));
        properties.put("big", Map.of("type", "long"));
        properties.put("ratio", Map.of("type", "double"));
        properties.put("flag", Map.of("type", "boolean"));
        properties.put("event.sequence", Map.of("type", "long"));
        for (int i = 1; i <= 4; i++)
            properties.put("stage_" + i, Map.of("type", "boolean"));
        return Map.of("properties", properties);
    }

    private static Map<String, Object> source(Case fixture, Event event) {
        Map<String, Object> source = new TreeMap<>();
        source.put("@timestamp", BASE.plusNanos(event.nanos).toString());
        if (event.keys.isEmpty() == false) source.put("key", event.keys.getFirst());
        source.put(
            "labels",
            event.keys.isEmpty() ? event.ordinal % 2 == 0 ? null : List.of() : event.keys.size() == 1 ? event.keys.getFirst() : event.keys
        );
        source.put("event_uid", event.id);
        source.put("route_key", event.route);
        source.put("raw_gate", event.accepted ? "allow" : "x");
        source.put("bucket", event.bucket);
        source.put("label", "PAYLOAD-" + event.id);
        source.put("n", event.ordinal);
        source.put("big", 9_007_199_254_740_993L + event.ordinal);
        source.put("ratio", (event.ordinal % 17) / 2.0);
        source.put("flag", event.ordinal % 2 == 0);
        source.put("tags", List.of("a-" + event.id, "b-" + event.id));
        if (event.ordinal % 3 != 0) source.put("optional", -event.ordinal);
        source.put("event", Map.of("category", "process", "sequence", event.ordinal));
        for (int i = 0; i < 4; i++)
            source.put("stage_" + (i + 1), (event.mask & (1 << i)) != 0);
        return source;
    }

    private static String nativeQuery(Case fixture, boolean outerComputation) {
        return nativeQuery(fixture, outerComputation, List.of());
    }

    private static String nativeQuery(Case fixture, boolean outerComputation, List<Long> incomingGaps) {
        List<String> stages = new ArrayList<>();
        for (int stage = 0; stage < fixture.stages; stage++) {
            String source = fixture.branches == 1
                ? "FROM " + fixture.indices().getFirst() + " METADATA _index, _id"
                : "FROM "
                    + fixture.indices()
                        .stream()
                        .map(index -> "(FROM " + index + " METADATA _index, _id)")
                        .collect(Collectors.joining(", "));
            String key = fixture.expand
                ? " | EVAL join_key = MV_APPEND(labels, labels) | MV_EXPAND join_key | EVAL join_key = TO_LOWER(join_key)"
                : " | EVAL join_key = TO_LOWER(key)";
            String filter = switch (fixture.filterMode) {
                case "none" -> "";
                case "eval" -> " | EVAL gate = LENGTH(raw_gate) | WHERE gate > 2";
                case "dissect" -> " | DISSECT raw_gate \"%{parsed_gate}\" | EVAL gate = LENGTH(parsed_gate) | WHERE gate > 2";
                case "grok" -> " | GROK raw_gate \"%{WORD:parsed_gate}\" | EVAL gate = LENGTH(parsed_gate) | WHERE gate > 2";
                default -> throw new IllegalArgumentException("unknown filter mode " + fixture.filterMode);
            };
            String computation = outerComputation ? "" : " | EVAL stage_value = n + 7, stage_name = TO_LOWER(label)";
            String payload = outerComputation ? "n, label" : "stage_value, stage_name";
            stages.add(
                "("
                    + source
                    + " | WHERE stage_"
                    + (stage + 1)
                    + " == true"
                    + filter
                    + key
                    + computation
                    + " | KEEP join_key, bucket, event_uid, route_key, _index, _id, "
                    + payload
                    + ", big, ratio, flag, tags, optional, @timestamp)"
                    + (incomingGaps.isEmpty() || incomingGaps.get(stage) == 0 ? "" : " WITHIN " + incomingGaps.get(stage) + " nanos")
            );
        }
        var query = new StringBuilder("SEQUENCE BY join_key");
        if (fixture.compound) query.append(", bucket");
        query.append(" WITH {\"maxspan\":\"")
            .append(fixture.spanMillis)
            .append("ms\"} PRIMARY ")
            .append(fixture.primary + 1)
            .append(' ')
            .append(String.join(", ", stages));
        if (outerComputation) {
            query.append(" | EVAL stage_value = n + 7, stage_name = TO_LOWER(label)");
        }
        List<String> output = new ArrayList<>(List.of("join_key"));
        if (fixture.compound) output.add("bucket");
        for (int stage = 0; stage < fixture.stages; stage++) {
            for (String field : List.of("_index", "_id", "event_uid", "route_key")) {
                String alias = "event_" + (stage + 1) + field;
                query.append(" | EVAL ").append(alias).append(" = ").append(reference(fixture, stage, field));
                output.add(alias);
            }
        }
        int other = fixture.primary == 0 ? fixture.stages - 1 : 0;
        query.append(" | EVAL other_value = ").append(reference(fixture, other, outerComputation ? "n" : "stage_value"));
        if (outerComputation) query.append(" + 7");
        output.addAll(List.of("stage_value", "stage_name", "big", "ratio", "flag", "tags", "optional", "@timestamp", "other_value"));
        query.append(" | KEEP ").append(String.join(", ", output));
        query.append(" | SORT join_key");
        if (fixture.compound) query.append(", bucket");
        for (int stage = 0; stage < fixture.stages; stage++)
            query.append(", event_").append(stage + 1).append("event_uid");
        return query.append(" | LIMIT 10000").toString();
    }

    private static String reference(Case fixture, int stage, String field) {
        return stage == fixture.primary ? field : "MEMBER(" + (stage + 1) + ", " + field + ")";
    }

    private static Map<String, Object> expectedResult(Case fixture, List<Selected> matches) {
        List<Column> columns = new ArrayList<>(List.of(new Column("join_key", "keyword")));
        if (fixture.compound) columns.add(new Column("bucket", "integer"));
        for (int stage = 0; stage < fixture.stages; stage++) {
            for (String name : List.of("_index", "_id", "event_uid", "route_key"))
                columns.add(new Column("event_" + (stage + 1) + name, "keyword"));
        }
        columns.addAll(
            List.of(
                new Column("stage_value", "integer"),
                new Column("stage_name", "keyword"),
                new Column("big", "long"),
                new Column("ratio", "double"),
                new Column("flag", "boolean"),
                new Column("tags", "keyword"),
                new Column("optional", "integer"),
                new Column("@timestamp", fixture.nanos ? "date_nanos" : "date"),
                new Column("other_value", "integer")
            )
        );
        List<List<Object>> rows = new ArrayList<>();
        for (Selected match : matches) {
            List<Object> row = new ArrayList<>();
            row.add(match.key.text);
            if (fixture.compound) row.add(Integer.toString(match.key.bucket));
            for (Event event : match.events)
                row.addAll(List.of(fixture.index(event), event.id, event.id, event.route));
            Event primary = match.events.get(fixture.primary);
            Event other = match.events.get(fixture.primary == 0 ? fixture.stages - 1 : 0);
            row.add(Integer.toString(primary.ordinal + 7));
            row.add("payload-" + primary.id);
            row.add(Long.toString(9_007_199_254_740_993L + primary.ordinal));
            row.add(Double.toString((primary.ordinal % 17) / 2.0));
            row.add(Boolean.toString(primary.ordinal % 2 == 0));
            row.add(List.of("a-" + primary.id, "b-" + primary.id));
            row.add(primary.ordinal % 3 == 0 ? null : Integer.toString(-primary.ordinal));
            row.add(epochNanos(BASE.plusNanos(primary.nanos)).toString());
            row.add(Integer.toString(other.ordinal + 7));
            rows.add(row);
        }
        return result(columns, rows);
    }

    /** Full prefix histories deliberately avoid the production matcher's compact state and expiry implementation. */
    private static List<Selected> historyOracle(Case fixture) {
        return historyOracle(fixture, List.of());
    }

    private static List<Selected> historyOracle(Case fixture, List<Long> incomingGaps) {
        List<Candidate> candidates = new ArrayList<>();
        for (Event event : fixture.events) {
            if (fixture.filtered && event.accepted == false) {
                continue;
            }
            List<String> keys = fixture.expand ? event.keys : event.keys.isEmpty() ? List.of() : List.of(event.keys.getFirst());
            for (int stage = 0; stage < fixture.stages; stage++) {
                if ((event.mask & (1 << stage)) == 0) continue;
                for (String key : keys.stream().map(s -> s.toLowerCase(Locale.ROOT)).distinct().sorted().toList()) {
                    candidates.add(new Candidate(stage, new Key(key, fixture.compound ? event.bucket : 0), event));
                }
            }
        }
        candidates.sort(
            Comparator.comparingLong((Candidate c) -> c.event.nanos)
                .thenComparing(Comparator.comparingInt((Candidate c) -> c.stage).reversed())
                .thenComparing(c -> c.key.text)
                .thenComparingInt(c -> c.key.bucket)
        );
        List<List<Prefix>> pending = new ArrayList<>();
        for (int stage = 0; stage < fixture.stages - 1; stage++)
            pending.add(new ArrayList<>());
        List<Selected> selected = new ArrayList<>();
        for (Candidate candidate : candidates) {
            if (candidate.stage == 0) {
                pending.getFirst().add(new Prefix(candidate.key, List.of(candidate.event)));
                continue;
            }
            List<Prefix> previous = pending.get(candidate.stage - 1);
            Prefix latest = null;
            for (Prefix prefix : previous) {
                if (prefix.key.equals(candidate.key) && prefix.events.getLast().nanos < candidate.event.nanos) latest = prefix;
            }
            previous.removeIf(prefix -> prefix.key.equals(candidate.key) && prefix.events.getLast().nanos < candidate.event.nanos);
            // A failed positive gap only worsens at later timestamps. Consuming that history is output-equivalent
            // to the engine retaining its failed prefix until ordinary total expiry or replacement.
            if (latest == null
                || BigInteger.valueOf(candidate.event.nanos)
                    .subtract(BigInteger.valueOf(latest.events.getFirst().nanos))
                    .compareTo(BigInteger.valueOf(fixture.spanNanos())) > 0
                || (incomingGaps.isEmpty() == false
                    && incomingGaps.get(candidate.stage) > 0
                    && BigInteger.valueOf(candidate.event.nanos)
                        .subtract(BigInteger.valueOf(latest.events.getLast().nanos))
                        .compareTo(BigInteger.valueOf(incomingGaps.get(candidate.stage))) > 0)) continue;
            List<Event> events = new ArrayList<>(latest.events);
            events.add(candidate.event);
            if (candidate.stage == fixture.stages - 1) selected.add(new Selected(fixture, candidate.key, List.copyOf(events)));
            else pending.get(candidate.stage).add(new Prefix(candidate.key, List.copyOf(events)));
        }
        selected.sort(
            Comparator.comparing((Selected s) -> s.key.text)
                .thenComparingInt(s -> s.key.bucket)
                .thenComparing(s -> s.events.stream().map(e -> e.id).collect(Collectors.joining("/")))
        );
        return selected;
    }

    /** A fixed coverage grid varies actual histories and branches; it is separate from the legacy20k generator. */
    private static LinkCase generatedLinkCase(int number) {
        int stages = 2 + number % 3;
        boolean nanos = number / 3 % 2 == 1;
        int family = number / 6 % 8;
        int variant = number / 48;
        long seed = SEED_BASE ^ (0x1A11L + 31_337L * number);
        Random random = new Random(seed);
        int branches = variant + 1;
        int keyCount = 2 + random.nextInt(3);
        long tick = nanos ? 1 : 1_000_000;
        long second = TimeUnit.SECONDS.toNanos(1);
        String profile = switch (family) {
            case 0 -> "link-absent";
            case 1 -> "link-equals-overall";
            case 2 -> "link-exact-inclusive";
            case 3 -> "link-one-mapping-tick-outside";
            case 4 -> "link-from-previous-not-first";
            case 5 -> "link-overall-still-applies";
            case 6 -> "link-replacement-and-consumption";
            case 7 -> "link-failed-gap-new-prefix-recovery";
            default -> throw new IllegalArgumentException("unknown link family " + family);
        };
        List<Long> incomingGaps = new ArrayList<>();
        incomingGaps.add(0L);
        for (int stage = 1; stage < stages; stage++) {
            incomingGaps.add(switch (family) {
                case 0 -> 0L;
                case 1 -> 30 * second;
                case 2, 3, 6, 7 -> 5 * second;
                case 4 -> (variant == 0 ? 10 : 5) * second;
                case 5 -> (stages == 2 ? 40 : 20) * second;
                default -> throw new IllegalArgumentException("unknown link family " + family);
            });
        }
        if (variant == 1 && (family == 4 || (family == 2 && stages > 2))) incomingGaps.set(1, 0L);
        List<Event> events = new ArrayList<>();
        for (int keyNumber = 0; keyNumber < keyCount; keyNumber++) {
            String key = String.format(Locale.ROOT, "l%02d", keyNumber);
            long origin = second + keyNumber * 100 * tick + random.nextInt(30) * tick;
            int earlierStarts = 1 + random.nextInt(5);
            for (int earlier = 0; earlier < earlierStarts; earlier++) {
                addLinkEvent(events, number, key, keyNumber % 2, branches, random, 0, origin - (earlierStarts - earlier) * tick);
            }
            if (family == 6) {
                addLinkEvent(events, number, key, keyNumber % 2, branches, random, 0, origin);
                if (stages == 2) {
                    addLinkEvent(events, number, key, keyNumber % 2, branches, random, 0, origin + second);
                    addLinkEvent(events, number, key, keyNumber % 2, branches, random, 1, origin + 2 * second);
                    addLinkEvent(events, number, key, keyNumber % 2, branches, random, 1, origin + 3 * second);
                } else {
                    addLinkEvent(events, number, key, keyNumber % 2, branches, random, 1, origin + second);
                    addLinkEvent(events, number, key, keyNumber % 2, branches, random, 0, origin + 2 * second);
                    addLinkEvent(events, number, key, keyNumber % 2, branches, random, 1, origin + 3 * second);
                    for (int stage = 2; stage < stages; stage++)
                        addLinkEvent(events, number, key, keyNumber % 2, branches, random, stage, origin + (stage + 2) * second);
                    addLinkEvent(events, number, key, keyNumber % 2, branches, random, stages - 1, origin + (stages + 2) * second);
                }
            } else if (family == 7) {
                addLinkEvent(events, number, key, keyNumber % 2, branches, random, 0, origin);
                addLinkEvent(events, number, key, keyNumber % 2, branches, random, 1, origin + 6 * second);
                addLinkEvent(events, number, key, keyNumber % 2, branches, random, 0, origin + 7 * second);
                for (int stage = 1; stage < stages; stage++)
                    addLinkEvent(events, number, key, keyNumber % 2, branches, random, stage, origin + (7 + stage) * second);
                addLinkEvent(events, number, key, keyNumber % 2, branches, random, stages - 1, origin + (7 + stages) * second);
            } else {
                for (int stage = 0; stage < stages; stage++) {
                    long offset = switch (family) {
                        case 0, 1 -> stage * (30 * second / (stages - 1));
                        case 2 -> stage * 5 * second;
                        case 3 -> stage * 5 * second + (stage == stages - 1 ? tick : 0);
                        case 4 -> stage == 0 ? 0 : ((variant == 0 ? 8 : 12) + 4L * (stage - 1)) * second;
                        case 5 -> stage * (stages == 2 ? 31 : stages == 3 ? 16 : 11) * second;
                        case 6, 7 -> throw new IllegalArgumentException("custom link history already handled");
                        default -> throw new IllegalArgumentException("unknown link family " + family);
                    };
                    addLinkEvent(events, number, key, keyNumber % 2, branches, random, stage, origin + offset);
                }
            }
        }
        java.util.Collections.shuffle(events, random);
        boolean eqlEligible = stages == 2 && (family == 0 || family == 1);
        Case fixture = new Case(
            String.format(Locale.ROOT, "link-case-%05d", number),
            seed,
            profile,
            stages,
            nanos,
            branches,
            random.nextInt(stages),
            false,
            variant == 1,
            false,
            "none",
            30_000,
            List.copyOf(events),
            eqlEligible,
            eqlEligible
                ? "two-stage absent/equal-overall controls; timestamps distinct"
                : "predeclared native-only scope; per-link predicates lack direct EQL syntax; no broader compatibility claim"
        );
        return new LinkCase(fixture, List.copyOf(incomingGaps), family == 3 || family == 5 ? 0 : keyCount);
    }

    private static void addLinkEvent(
        List<Event> events,
        int number,
        String key,
        int bucket,
        int branches,
        Random random,
        int stage,
        long nanos
    ) {
        int ordinal = events.size();
        String id = String.format(Locale.ROOT, "link-%05d-e%03d", number, ordinal);
        events.add(new Event(id, "route-" + ordinal, random.nextInt(branches), ordinal, List.of(key), bucket, 1 << stage, nanos, true));
    }

    private static Case generatedCase(int number) {
        long seed = SEED_BASE + 104_729L * number;
        Random random = new Random(seed);
        if (number < 36) {
            return boundary(number, seed, 2 + number / 12, number / 6 % 2 == 1, BOUNDARIES[number % BOUNDARIES.length]);
        }
        if (number % 101 == 0) {
            return boundary(number, seed, 2 + random.nextInt(3), random.nextBoolean(), BOUNDARIES[random.nextInt(BOUNDARIES.length)]);
        }
        int kind = (number - 36) % 6;
        int stages = switch (kind) {
            case 0, 5 -> 2;
            case 1, 2 -> 3 + random.nextInt(2);
            case 3, 4 -> 2 + random.nextInt(3);
            default -> throw new AssertionError(kind);
        };
        boolean nanos = random.nextBoolean();
        int branches = 1 + random.nextInt(2);
        boolean compound = random.nextBoolean();
        boolean expand = kind == 3;
        boolean filtered = kind == 4;
        String filterMode = filtered ? switch (((number - 36) / 6) % 3) {
            case 0 -> "eval";
            case 1 -> "dissect";
            case 2 -> "grok";
            default -> throw new AssertionError();
        } : "none";
        long tick = nanos ? 1L : 1_000_000L;
        int spanMillis = kind == 1 || kind == 4
            ? random.nextBoolean() ? 1_000 : 30_000
            : new int[] { 1, 10, 1_000, 30_000 }[random.nextInt(4)];
        long span = TimeUnit.MILLISECONDS.toNanos(spanMillis);
        List<Event> events = new ArrayList<>();
        String profile;
        boolean eqlEligible;
        String eqlReason;
        if (kind == 1) {
            profile = "fenced-multistage-repeated-episodes";
            long clock = 0;
            for (int key = 0, keys = 1 + random.nextInt(5); key < keys; key++) {
                for (int episode = 0, episodes = 2 + random.nextInt(7); episode < episodes; episode++) {
                    clock += span + 20 * tick;
                    for (int competing = 0, starts = 1 + random.nextInt(4); competing < starts; competing++) {
                        add(events, number, random.nextInt(branches), List.of("key" + key), key % 3, 1, clock, true);
                        clock += tick;
                    }
                    for (int stage = 1; stage < stages; stage++) {
                        clock += (1 + random.nextInt(5)) * tick;
                        if (stage == stages - 1 && episode % 4 == 3) {
                            clock += span + tick;
                        }
                        add(events, number, random.nextInt(branches), List.of("key" + key), key % 3, 1 << stage, clock, true);
                    }
                    clock += tick;
                    add(events, number, random.nextInt(branches), List.of("key" + key), key % 3, 1 << (stages - 1), clock, true);
                }
            }
            eqlEligible = true;
            eqlReason = "eligible: scalar tie-free isolated episodes; starts compete before one ordered middle chain";
        } else {
            profile = switch (kind) {
                case 0 -> "arbitrary-two-stage-competition";
                case 2 -> "unrestricted-multistage-overlap";
                case 3 -> "expanded-duplicate-null-and-empty-keys";
                case 4 -> "computed-filter-and-later-stage-tail";
                case 5 -> "no-starts-and-unmatched-events";
                default -> throw new AssertionError(kind);
            };
            long clock = 0;
            if (kind != 5) {
                for (int stage = 0; stage < stages; stage++) {
                    add(events, number, random.nextInt(branches), List.of("anchor"), 0, 1 << stage, clock, true);
                    clock += tick;
                }
            }
            int size = kind == 4 ? 2 * branches * BOUNDARIES[3 + random.nextInt(3)] : 32 + random.nextInt(161);
            int keyDomain = 1 + random.nextInt(9);
            for (int i = 0; i < size; i++) {
                clock += (1 + random.nextInt(17)) * tick;
                if (i > 0 && i % (13 + number % 17) == 0) {
                    clock += span + tick;
                }
                String key = "key" + random.nextInt(i % 3 == 0 ? 1 : keyDomain);
                List<String> keys = List.of(key);
                if (expand) {
                    keys = switch (random.nextInt(6)) {
                        case 0 -> List.of();
                        case 1 -> List.of(key);
                        case 2 -> List.of(key, key);
                        case 3 -> List.of(key, key.toUpperCase(Locale.ROOT));
                        case 4 -> List.of(key, "shared");
                        case 5 -> List.of(key, "shared", key);
                        default -> throw new AssertionError();
                    };
                }
                int mask = kind == 5 ? random.nextBoolean() ? 2 : 0 : kind == 4 ? (1 << stages) - 1 : random.nextInt(1 << stages);
                boolean accepted = kind != 4 || random.nextInt(5) == 0;
                if (kind == 4 && i >= size / 2) {
                    mask = 1 << (stages - 1);
                    accepted = false;
                }
                add(events, number, kind == 4 ? i % branches : random.nextInt(branches), keys, random.nextInt(3), mask, clock, accepted);
            }
            eqlEligible = kind == 0 || kind == 5;
            eqlReason = switch (kind) {
                case 0, 5 -> "eligible: arbitrary tie-free scalar two-stage history with the declared positive-stage selection";
                case 2 ->
                    "excluded before execution: unrestricted overlapping multi-stage prefixes use the native selected-predecessor contract";
                case 3 -> "excluded before execution: explicit native duplicate expansion and normalization";
                case 4 -> "excluded before execution: native computed stage filtering";
                default -> throw new AssertionError(kind);
            };
        }
        Collections.shuffle(events, random);
        return new Case(
            name(number),
            seed,
            profile,
            stages,
            nanos,
            branches,
            random.nextInt(stages),
            expand,
            compound,
            filtered,
            filterMode,
            spanMillis,
            List.copyOf(events),
            eqlEligible,
            eqlReason
        );
    }

    private static Case boundary(int number, long seed, int stages, boolean nanos, int rows) {
        List<Event> events = new ArrayList<>();
        int branches = number % 2 + 1;
        long tick = nanos ? 1L : 1_000_000L;
        long span = TimeUnit.SECONDS.toNanos(30);
        for (int stage = 0; stage < stages; stage++) {
            for (int key = 0; key < rows * branches; key++) {
                long instant = key * tick + stage * (span / (stages - 1));
                if (stage == stages - 1) {
                    instant += (key % 3 - 1) * tick;
                }
                add(events, number, key % branches, List.of(String.format(Locale.ROOT, "k%04d", key)), key % 3, 1 << stage, instant, true);
            }
        }
        Collections.shuffle(events, new Random(seed));
        return new Case(
            name(number),
            seed,
            "page-boundary-" + rows + "-inside-inclusive-outside",
            stages,
            nanos,
            branches,
            number % stages,
            false,
            number % 3 == 0,
            false,
            "none",
            30_000,
            List.copyOf(events),
            true,
            "eligible: tie-free scalar one-chain-per-key page boundary"
        );
    }

    private static void add(
        List<Event> events,
        int number,
        int branch,
        List<String> keys,
        int bucket,
        int mask,
        long nanos,
        boolean accepted
    ) {
        int ordinal = events.size();
        String id = String.format(Locale.ROOT, "c%05d-e%05d", number, ordinal);
        events.add(new Event(id, "route-" + ordinal, branch, ordinal, List.copyOf(keys), bucket, mask, nanos, accepted));
    }

    private static String name(int number) {
        return String.format(Locale.ROOT, "case-%05d", number);
    }

    /** Literal histories pin the feature's meaning before the randomized integration profiles run. */
    private static Map<String, Object> assertLinkOracleContracts() {
        long second = TimeUnit.SECONDS.toNanos(1);
        Map<String, Object> contracts = new LinkedHashMap<>();
        Case totalEdge = manualCase(2, List.of(manualEvent("A", 1, 0, 0), manualEvent("B", 2, 30 * second, 1)));
        linkOracleContract(contracts, "absent", totalEdge, List.of(0L, 0L), List.of("k#0:A,B"));
        linkOracleContract(contracts, "equal-overall", totalEdge, List.of(0L, 30 * second), List.of("k#0:A,B"));
        linkOracleContract(
            contracts,
            "inclusive",
            manualCase(3, List.of(manualEvent("A", 1, 0, 0), manualEvent("B", 2, 5 * second, 1), manualEvent("C", 4, 10 * second, 2))),
            List.of(0L, 5 * second, 5 * second),
            List.of("k#0:A,B,C")
        );
        linkOracleContract(
            contracts,
            "one-nanosecond-outside",
            manualCase(3, List.of(manualEvent("A", 1, 0, 0), manualEvent("B", 2, 5 * second, 1), manualEvent("C", 4, 10 * second + 1, 2))),
            List.of(0L, 5 * second, 5 * second),
            List.of()
        );
        linkOracleContract(
            contracts,
            "previous-not-first",
            manualCase(3, List.of(manualEvent("A", 1, 0, 0), manualEvent("B", 2, 8 * second, 1), manualEvent("C", 4, 12 * second, 2))),
            List.of(0L, 10 * second, 10 * second),
            List.of("k#0:A,B,C")
        );
        linkOracleContract(
            contracts,
            "absent-middle-link",
            manualCase(3, List.of(manualEvent("A", 1, 0, 0), manualEvent("B", 2, 12 * second, 1), manualEvent("C", 4, 16 * second, 2))),
            List.of(0L, 0L, 5 * second),
            List.of("k#0:A,B,C")
        );
        linkOracleContract(
            contracts,
            "overall-still-applies",
            manualCase(3, List.of(manualEvent("A", 1, 0, 0), manualEvent("B", 2, 16 * second, 1), manualEvent("C", 4, 32 * second, 2))),
            List.of(0L, 20 * second, 20 * second),
            List.of()
        );
        linkOracleContract(
            contracts,
            "replacement",
            manualCase(
                3,
                List.of(
                    manualEvent("A1", 1, 0, 0),
                    manualEvent("B1", 2, second, 1),
                    manualEvent("A2", 1, 2 * second, 2),
                    manualEvent("B2", 2, 3 * second, 3),
                    manualEvent("C", 4, 4 * second, 4)
                )
            ),
            List.of(0L, 5 * second, 5 * second),
            List.of("k#0:A2,B2,C")
        );
        linkOracleContract(
            contracts,
            "consumption",
            manualCase(2, List.of(manualEvent("A", 1, 0, 0), manualEvent("B1", 2, second, 1), manualEvent("B2", 2, 2 * second, 2))),
            List.of(0L, 5 * second),
            List.of("k#0:A,B1")
        );
        linkOracleContract(
            contracts,
            "failed-gap-new-prefix-recovery",
            manualCase(
                3,
                List.of(
                    manualEvent("A1", 1, 0, 0),
                    manualEvent("B-failed", 2, 6 * second, 1),
                    manualEvent("A2", 1, 7 * second, 2),
                    manualEvent("B2", 2, 8 * second, 3),
                    manualEvent("C", 4, 9 * second, 4)
                )
            ),
            List.of(0L, 5 * second, 5 * second),
            List.of("k#0:A2,B2,C")
        );
        return contracts;
    }

    private static void linkOracleContract(
        Map<String, Object> contracts,
        String name,
        Case fixture,
        List<Long> incomingGaps,
        List<String> expected
    ) {
        assertEquals(name, expected, selectedNames(historyOracle(fixture, incomingGaps)));
        contracts.put(
            name,
            Map.of(
                "incoming_max_gap_nanos",
                incomingGaps.stream().map(Object::toString).toList(),
                "events",
                fixture.events.stream().map(event -> List.of(event.id, event.mask, Long.toString(event.nanos))).toList(),
                "expected_selected_ids",
                expected
            )
        );
    }

    /** Hand-worked definitions pin replacement, consumption, strict ordering and inclusive total span independently. */
    private static void assertOracleContracts() {
        Case latest = manualCase(
            2,
            List.of(manualEvent("A1", 1, 0, 0), manualEvent("A2", 1, 1, 1), manualEvent("B", 2, 2, 2), manualEvent("B-unused", 2, 3, 3))
        );
        assertEquals(List.of("k#0:A2,B"), selectedNames(historyOracle(latest)));
        Case replacement = manualCase(
            3,
            List.of(
                manualEvent("A1", 1, 0, 0),
                manualEvent("B1", 2, 1, 1),
                manualEvent("A2", 1, 2, 2),
                manualEvent("B2", 2, 3, 3),
                manualEvent("C", 4, 4, 4),
                manualEvent("C-unused", 4, 5, 5)
            )
        );
        assertEquals(List.of("k#0:A2,B2,C"), selectedNames(historyOracle(replacement)));
        Case advanced = manualCase(
            3,
            List.of(manualEvent("A1", 1, 0, 0), manualEvent("B1", 2, 1, 1), manualEvent("A2", 1, 2, 2), manualEvent("C", 4, 3, 3))
        );
        assertEquals(List.of("k#0:A1,B1,C"), selectedNames(historyOracle(advanced)));
        Case unrelatedWatermark = manualCase(
            2,
            List.of(
                manualEvent("A-k", 1, 0, 0),
                new Event("A-u", "u1", 0, 1, List.of("u"), 0, 1, 1, true),
                new Event("B-u", "u2", 0, 2, List.of("u"), 0, 2, 2, true),
                new Event("watermark-v", "v", 0, 3, List.of("v"), 0, 2, TimeUnit.SECONDS.toNanos(31), true),
                manualEvent("B-k-expired", 2, TimeUnit.SECONDS.toNanos(32), 4)
            )
        );
        assertEquals(List.of("u#0:A-u,B-u"), selectedNames(historyOracle(unrelatedWatermark)));
        Case overlap = manualCase(2, List.of(manualEvent("A", 1, 0, 0), manualEvent("both", 3, 1, 1), manualEvent("B", 2, 2, 2)));
        assertEquals(List.of("k#0:A,both", "k#0:both,B"), selectedNames(historyOracle(overlap)));
        Case inclusive = manualCase(2, List.of(manualEvent("A", 1, 0, 0), manualEvent("B", 2, TimeUnit.SECONDS.toNanos(30), 1)));
        assertEquals(List.of("k#0:A,B"), selectedNames(historyOracle(inclusive)));
        Case outside = manualCase(2, List.of(manualEvent("A", 1, 0, 0), manualEvent("B", 2, TimeUnit.SECONDS.toNanos(30) + 1, 1)));
        assertTrue(historyOracle(outside).isEmpty());
        Case expanded = new Case(
            "manual-expanded",
            0,
            "manual",
            2,
            true,
            1,
            0,
            true,
            false,
            false,
            "none",
            30_000,
            List.of(
                new Event("A", "a", 0, 0, List.of("a", "A", "b", "b"), 0, 1, 0, true),
                new Event("B", "b", 0, 1, List.of("a", "b"), 0, 2, 1, true)
            ),
            false,
            "manual native expansion"
        );
        assertEquals(List.of("a#0:A,B", "b#0:A,B"), selectedNames(historyOracle(expanded)));
        Case missing = manualCase(
            2,
            List.of(new Event("A", "a", 0, 0, List.of(), 0, 1, 0, true), new Event("B", "b", 0, 1, List.of(), 0, 2, 1, true))
        );
        assertTrue(historyOracle(missing).isEmpty());
    }

    private static Case manualCase(int stages, List<Event> events) {
        return new Case("manual", 0, "manual", stages, true, 1, 0, false, false, false, "none", 30_000, events, false, "manual");
    }

    private static Event manualEvent(String id, int mask, long instant, int ordinal) {
        return new Event(id, id, 0, ordinal, List.of("k"), 0, mask, instant, true);
    }

    private static List<String> selectedNames(List<Selected> selected) {
        return selected.stream()
            .map(s -> s.key.text + "#" + s.key.bucket + ":" + s.events.stream().map(Event::id).collect(Collectors.joining(",")))
            .toList();
    }

    private static Map<String, Object> fixtureDocument(Case fixture) {
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("case", fixture.name);
        input.put("seed", Long.toUnsignedString(fixture.seed));
        input.put("profile", fixture.profile);
        input.put("stages", fixture.stages);
        input.put("branches", fixture.branches);
        input.put("timestamp_domain", fixture.nanos ? "date_nanos" : "date");
        input.put("maxspan_nanos", Long.toString(fixture.spanNanos()));
        input.put("primary", fixture.primary + 1);
        input.put("expanded_keys", fixture.expand);
        input.put("compound_keys", fixture.compound);
        input.put("computed_stage_filter", fixture.filtered);
        input.put("filter_mode", fixture.filterMode);
        input.put("eql_eligible", fixture.eqlEligible);
        input.put("eql_reason", fixture.eqlReason);
        input.put(
            "documents",
            fixture.events.stream()
                .map(e -> Map.of("_index", fixture.index(e), "_id", e.id, "_routing", e.route, "_source", source(fixture, e)))
                .toList()
        );
        input.put("mappings", mapping(fixture));
        input.put("settings", Map.of("index.number_of_shards", 1, "index.number_of_replicas", 0));
        return input;
    }

    private static Map<String, Object> identityDocument(Selected selected) {
        List<String> keys = new ArrayList<>(List.of(selected.key.text));
        if (selected.fixture.compound) keys.add(Integer.toString(selected.key.bucket));
        return Map.of("keys", keys, "events", selected.events.stream().map(e -> selected.fixture.index(e) + "/" + e.id).toList());
    }

    private static String identitySortKey(Object value) {
        return json(value);
    }

    private static Map<String, Object> result(List<Column> columns, List<List<Object>> rows) {
        return Map.of(
            "columns",
            columns.stream().map(c -> Map.of("name", c.name, "type", c.type)).toList(),
            "rows",
            rows,
            "partial",
            false,
            "running",
            false,
            "warnings",
            List.of()
        );
    }

    private static Object cell(String type, Object value) {
        if (value == null) return null;
        if (value instanceof List<?> values) return values.stream().map(v -> cell(type, v)).toList();
        if (type.equals("date") || type.equals("date_nanos")) return epochNanos(Instant.parse(value.toString())).toString();
        return value.toString();
    }

    /** Diagnostic kinds retain primitive representation without duplicating the full actual table. */
    private static void collectKinds(Object value, java.util.Set<String> kinds) {
        if (value == null) {
            kinds.add("null");
        } else if (value instanceof List<?> values) {
            kinds.add("list");
            values.forEach(item -> collectKinds(item, kinds));
        } else {
            kinds.add(value.getClass().getName());
        }
    }

    private static void writeCase(Path path, Object value) throws java.io.IOException {
        String encoded = json(value);
        assertTrue("case evidence exceeds the admitted bound", encoded.getBytes(StandardCharsets.UTF_8).length <= MAX_CASE_BYTES);
        Files.writeString(path, encoded, StandardCharsets.UTF_8);
    }

    private static BigInteger epochNanos(Instant instant) {
        return BigInteger.valueOf(instant.getEpochSecond()).multiply(BILLION).add(BigInteger.valueOf(instant.getNano()));
    }

    /** Canonical map order makes fixture hashes independent of JVM map iteration randomization. */
    private static Object canonical(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> sorted = new TreeMap<>();
            map.forEach((key, item) -> sorted.put(key.toString(), canonical(item)));
            return sorted;
        }
        if (value instanceof List<?> list) return list.stream().map(SequenceSemanticCampaignIT::canonical).toList();
        return value;
    }

    private static String json(Object value) {
        try (var builder = JsonXContent.contentBuilder()) {
            builder.value(canonical(value));
            return BytesReference.bytes(builder).utf8ToString();
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    private static void write(Path path, Object value) throws java.io.IOException {
        Files.writeString(path, json(value), StandardCharsets.UTF_8);
    }

    private static String sha(String value) throws Exception {
        return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    /** Real PIT, task, block and both request/EQL breaker checks run between queries as well as fixture cleanup. */
    private void assertReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class))
                assertEquals(0, service.getActiveContexts());
            for (CircuitBreakerService service : internalCluster().getInstances(CircuitBreakerService.class)) {
                assertEquals("native request reservations retained", 0, service.getBreaker(CircuitBreaker.REQUEST).getUsed());
                assertEquals("EQL matcher reservations retained", 0, service.getBreaker(EqlPlugin.CIRCUIT_BREAKER_NAME).getUsed());
            }
            assertTrue(
                client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(EqlSearchAction.NAME, EsqlQueryAction.NAME, DriverTaskRunner.ACTION_NAME)
                    .get()
                    .getTasks()
                    .isEmpty()
            );
        });
        ensureBlocksReleased();
    }

    private record Column(String name, String type) {}

    private record Event(
        String id,
        String route,
        int branch,
        int ordinal,
        List<String> keys,
        int bucket,
        int mask,
        long nanos,
        boolean accepted
    ) {}

    private record Key(String text, int bucket) {}

    /** Per-link data stays outside Case so every legacy fixture and topology document remains unchanged. */
    private record LinkCase(Case fixture, List<Long> incomingGaps, int expectedMatches) {
        private LinkCase {
            incomingGaps = List.copyOf(incomingGaps);
            if (incomingGaps.size() != fixture.stages || incomingGaps.getFirst() != 0 || incomingGaps.stream().anyMatch(gap -> gap < 0))
                throw new IllegalArgumentException("invalid incoming-link contract");
        }
    }

    private record Candidate(int stage, Key key, Event event) {}

    private record Prefix(Key key, List<Event> events) {}

    private record Selected(Case fixture, Key key, List<Event> events) {}

    /** Immutable generation choices are frozen before either query engine runs. */
    private record Case(
        String name,
        long seed,
        String profile,
        int stages,
        boolean nanos,
        int branches,
        int primary,
        boolean expand,
        boolean compound,
        boolean filtered,
        String filterMode,
        int spanMillis,
        List<Event> events,
        boolean eqlEligible,
        String eqlReason
    ) {
        private List<String> indices() {
            return java.util.stream.IntStream.range(0, branches).mapToObj(branch -> "sequence-semantic-" + name + "-b" + branch).toList();
        }

        private String index(Event event) {
            return indices().get(event.branch);
        }

        private long spanNanos() {
            return TimeUnit.MILLISECONDS.toNanos(spanMillis);
        }

        private Case withEvents(List<Event> reducedEvents) {
            return new Case(
                name,
                seed,
                profile,
                stages,
                nanos,
                branches,
                primary,
                expand,
                compound,
                filtered,
                filterMode,
                spanMillis,
                List.copyOf(reducedEvents),
                eqlEligible,
                eqlReason
            );
        }
    }

    /** Counts actual attempts and responses separately from generated cases and JUnit invocations. */
    private static class QueryCounts {
        private long nativeAttempts;
        private long nativeResponses;
        private long eqlAttempts;
        private long eqlResponses;

        private Map<String, Object> document() {
            return Map.of(
                "native_attempts",
                nativeAttempts,
                "native_responses",
                nativeResponses,
                "eql_attempts",
                eqlAttempts,
                "eql_responses",
                eqlResponses
            );
        }
    }

    /** A named semantic lane permits shrinking only the originally observed mismatch. */
    private static class SemanticMismatch extends AssertionError {
        private final String lane;

        private SemanticMismatch(String lane, Object expected, Object actual) {
            super(lane + " differs: expected=" + json(expected) + "; actual=" + json(actual));
            this.lane = lane;
        }
    }
}
