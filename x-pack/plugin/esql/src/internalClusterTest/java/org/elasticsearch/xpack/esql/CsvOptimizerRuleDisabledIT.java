/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LookupLogicalOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LookupPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.OptimizerStage;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.rule.MandatoryRule;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Differential correctness oracle for the ES|QL optimizer: runs the same csv-spec corpus as {@link CsvIT} but with
 * <strong>one randomly-chosen optional optimizer rule disabled per test case</strong>, asserting that the results
 * still match the csv-spec expected values.
 *
 * <h2>Rationale</h2>
 * Optimizer rules are supposed to be <em>semantics-preserving</em>: a query's observable output must be identical
 * whether or not any given optional rule runs. If disabling a rule changes the result, either the rule is buggy (as
 * with {@code InferIsNotNull} in issue #155101) or the csv-spec expected value already encodes the bug. Either way it
 * is a discrepancy worth investigating.
 *
 * <h2>Rule eligibility</h2>
 * A rule is eligible for random disabling unless it implements {@link MandatoryRule}. Mandatory rules are rules whose
 * removal is not semantics-preserving by design — for example structural rewrites that subsequent rules depend on.
 * Marking a rule as mandatory is a deliberate, per-rule decision: rules start unmarked, and triage after a suite run
 * determines which failures are mandatory-rule artefacts (add the marker) versus genuine bugs (file an issue, leave
 * the rule unmarked).
 *
 * <h2>Pragma mechanism</h2>
 * Rule disabling is communicated to the cluster via the snapshot-only {@code disable_optimizer_rules}
 * {@link QueryPragmas pragma}. Entries are stage-scoped ({@code "<stage-key>:<RuleSimpleName>"}) so a rule can be
 * targeted in one optimizer stage independently of others. The pragma reaches all six optimizer stages
 * transparently via the request-borne {@link org.elasticsearch.xpack.esql.session.Configuration}.
 *
 * <h2>Per-rule pinning</h2>
 * To reproduce a failure or investigate a specific rule, pass
 * {@code -Dtests.esql.disable_optimizer_rule=<stage-key>:<RuleName>} (or just {@code <RuleName>} for all stages).
 * The strategy will use that entry for every test in the run instead of picking randomly.
 *
 * <h2>Bootstrap workflow</h2>
 * With few or no rules marked mandatory the suite is expected to fail at first — those failures are the discovery
 * signal. For each failure, reproduce deterministically by pinning the rule, then decide: mandatory rule (add
 * {@link MandatoryRule} marker with a comment) or genuine correctness bug (file an issue, leave the rule unmarked).
 * Repeat until the suite is green modulo tracked bugs.
 *
 * <p>This variant is snapshot-only: on release builds the {@code disable_optimizer_rules} pragma is a no-op, so the
 * {@code assumeTrue} gate in {@link #installDisableOptimizerRuleStrategy()} skips the whole class.</p>
 */
public class CsvOptimizerRuleDisabledIT extends CsvIT {

    private static final Logger logger = LogManager.getLogger(CsvOptimizerRuleDisabledIT.class);

    /** System property to pin a specific rule for the whole run, e.g. {@code local_logical:InferIsNotNull}. */
    public static final String PIN_RULE_PROPERTY = "tests.esql.disable_optimizer_rule";

    /**
     * System property for list-mode: write the full {@code <stage-key>:<RuleName>} catalog (including rules marked
     * {@link MandatoryRule}) to the given file path, one entry per line, then skip test execution. Used by the
     * exhaustive-sweep driver to discover the pair list before launching per-rule runs.
     */
    public static final String LIST_RULES_PROPERTY = "tests.esql.list_optimizer_rules_file";

    /**
     * Per-pragma-value launch counter; keyed by the full pragma string (e.g.
     * {@code "local_logical:InferIsNotNull"}) so the {@link #logOptimizerRuleDisabledSummary summary} can
     * break down how often each rule was exercised across the test JVM's lifetime.
     */
    private static final ConcurrentHashMap<String, AtomicInteger> LAUNCHED_COUNTS = new ConcurrentHashMap<>();

    /**
     * Immutable candidate pool built once in {@link #installDisableOptimizerRuleStrategy()}. Each entry is a
     * {@code (stage, ruleSimpleName)} pair representing a rule that can be safely disabled without guaranteed
     * semantics changes (i.e. the rule does not implement {@link MandatoryRule}).
     */
    private static List<CandidateRule> candidatePool;

    public CsvOptimizerRuleDisabledIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvSpecReader.CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    /**
     * Installs the disable-optimizer-rule strategy after first skipping the whole variant on release builds.
     *
     * <p>Runs after {@link CsvIT#setupCluster()} (JUnit guarantees the superclass {@code @BeforeClass} runs first)
     * and replaces the identity strategy with one that adds a {@code disable_optimizer_rules} pragma to every
     * test.</p>
     *
     * <p>List-mode: if {@link #LIST_RULES_PROPERTY} is set, the full {@code (stage:rule)} catalog is written to the
     * specified file (including {@link MandatoryRule}-marked rules) and test execution is skipped. This is used by
     * the exhaustive-sweep driver to obtain the pair list before launching per-rule corpus runs.</p>
     */
    @BeforeClass
    public static void installDisableOptimizerRuleStrategy() throws IOException {
        assumeTrue("disable_optimizer_rules pragma is a no-op on release builds; skipping variant", Build.current().isSnapshot());

        String listFile = System.getProperty(LIST_RULES_PROPERTY);
        if (listFile != null && listFile.isBlank() == false) {
            List<String> catalog = buildAllRulesCatalog();
            Files.writeString(Path.of(listFile.trim()), String.join("\n", catalog) + "\n");
            logger.info("optimizer-rule-disabled: wrote {} rule entries to {}", catalog.size(), listFile.trim());
            assumeTrue("list-mode: rule catalog written to " + listFile.trim() + "; skipping test execution", false);
        }

        candidatePool = buildCandidatePool();
        assumeTrue(
            "No optional optimizer rules found — all rules implement MandatoryRule; skipping variant",
            candidatePool.isEmpty() == false
        );
        logger.info("optimizer-rule-disabled: candidate pool has {} entries (across all six optimizer stages)", candidatePool.size());
        indexLoadStrategy = new DisableOptimizerRuleStrategy(candidatePool);
    }

    @AfterClass
    public static void logOptimizerRuleDisabledSummary() {
        int total = 0;
        for (AtomicInteger c : LAUNCHED_COUNTS.values()) {
            total += c.get();
        }
        logger.info("optimizer-rule-disabled summary: total-launched={}", total);
        LAUNCHED_COUNTS.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(e -> logger.info("optimizer-rule-disabled summary: rule={} count={}", e.getKey(), e.getValue().get()));
    }

    // ── Candidate pool enumeration ────────────────────────────────────────────────────────────────

    private record CandidateRule(OptimizerStage stage, String ruleName) {}

    /**
     * Enumerates every rule across the six optimizer stages and returns those that do not implement
     * {@link MandatoryRule}. Uses reflection to access the package-private {@code RULES} field of each optimizer
     * class, which is acceptable in test code.
     */
    private static List<CandidateRule> buildCandidatePool() {
        List<CandidateRule> candidates = new ArrayList<>();
        addRulesFromOptimizer(candidates, LogicalPlanOptimizer.class, OptimizerStage.GLOBAL_LOGICAL, false);
        addRulesFromOptimizer(candidates, LocalLogicalPlanOptimizer.class, OptimizerStage.LOCAL_LOGICAL, false);
        addRulesFromOptimizer(candidates, PhysicalPlanOptimizer.class, OptimizerStage.GLOBAL_PHYSICAL, false);
        addRulesFromOptimizer(candidates, LocalPhysicalPlanOptimizer.class, OptimizerStage.LOCAL_PHYSICAL, false);
        addRulesFromOptimizer(candidates, LookupLogicalOptimizer.class, OptimizerStage.LOOKUP_LOGICAL, false);
        addRulesFromOptimizer(candidates, LookupPhysicalPlanOptimizer.class, OptimizerStage.LOOKUP_PHYSICAL, false);
        return List.copyOf(candidates);
    }

    /**
     * Enumerates every rule across all six optimizer stages, including those marked {@link MandatoryRule}, and
     * returns a sorted, deduplicated list of {@code "<stage-key>:<RuleName>"} strings. Used by list-mode to produce
     * the complete pair catalog for the exhaustive-sweep driver.
     */
    private static List<String> buildAllRulesCatalog() {
        List<CandidateRule> all = new ArrayList<>();
        addRulesFromOptimizer(all, LogicalPlanOptimizer.class, OptimizerStage.GLOBAL_LOGICAL, true);
        addRulesFromOptimizer(all, LocalLogicalPlanOptimizer.class, OptimizerStage.LOCAL_LOGICAL, true);
        addRulesFromOptimizer(all, PhysicalPlanOptimizer.class, OptimizerStage.GLOBAL_PHYSICAL, true);
        addRulesFromOptimizer(all, LocalPhysicalPlanOptimizer.class, OptimizerStage.LOCAL_PHYSICAL, true);
        addRulesFromOptimizer(all, LookupLogicalOptimizer.class, OptimizerStage.LOOKUP_LOGICAL, true);
        addRulesFromOptimizer(all, LookupPhysicalPlanOptimizer.class, OptimizerStage.LOOKUP_PHYSICAL, true);
        return all.stream().map(r -> r.stage().pragmaKey() + ":" + r.ruleName()).distinct().sorted().collect(Collectors.toList());
    }

    /**
     * Accesses the static {@code RULES} field of {@code optimizerClass} via reflection, iterates its batches, and
     * adds each rule to {@code candidates}. When {@code includeMandatory} is {@code false}, rules whose class
     * implements {@link MandatoryRule} are skipped (normal random-suite behaviour). When {@code true}, all rules are
     * included (used by {@link #buildAllRulesCatalog()} for the exhaustive-sweep list-mode).
     *
     * <p>Uses raw types to avoid unchecked-cast noise; the casts are safe because we know the field type.</p>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void addRulesFromOptimizer(
        List<CandidateRule> candidates,
        Class<?> optimizerClass,
        OptimizerStage stage,
        boolean includeMandatory
    ) {
        try {
            Field rulesField = optimizerClass.getDeclaredField("RULES");
            rulesField.setAccessible(true);
            Iterable<?> batches = (Iterable<?>) rulesField.get(null);
            for (Object batchObj : batches) {
                RuleExecutor.Batch batch = (RuleExecutor.Batch) batchObj;
                for (Object ruleObj : batch.rules()) {
                    if (includeMandatory || MandatoryRule.class.isAssignableFrom(ruleObj.getClass()) == false) {
                        candidates.add(new CandidateRule(stage, ruleObj.getClass().getSimpleName()));
                    }
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(
                "Failed to enumerate optimizer rules from "
                    + optimizerClass.getSimpleName()
                    + "; "
                    + "this is likely because the RULES field was renamed or its visibility changed",
                e
            );
        }
    }

    // ── Strategy ─────────────────────────────────────────────────────────────────────────────────

    private static final class DisableOptimizerRuleStrategy implements IndexLoadStrategy {

        private final List<CandidateRule> candidatePool;

        /**
         * Pragma value pinned for the whole run via {@link #PIN_RULE_PROPERTY}, or {@code null} when each test picks
         * randomly from the pool.
         */
        private final String pinnedPragmaValue;

        DisableOptimizerRuleStrategy(List<CandidateRule> candidatePool) {
            this.candidatePool = candidatePool;
            String pinned = System.getProperty(PIN_RULE_PROPERTY);
            this.pinnedPragmaValue = (pinned != null && pinned.isBlank() == false) ? pinned.trim() : null;
            if (this.pinnedPragmaValue != null) {
                logger.info("optimizer-rule-disabled: pinned rule for entire run: {}", this.pinnedPragmaValue);
            }
        }

        @Override
        public String transformMapping(CsvTestsDataLoader.TestDataset dataset, String originalMapping) {
            return originalMapping;
        }

        @Override
        public Settings transformSettings(CsvTestsDataLoader.TestDataset dataset, Settings settings) {
            return settings;
        }

        @Override
        public String transformDocument(CsvTestsDataLoader.TestDataset dataset, String originalDocumentJson) {
            return originalDocumentJson;
        }

        /**
         * Returns the original query unchanged but adds the {@code disable_optimizer_rules} pragma so that one
         * optimizer rule is skipped during execution. The rule is either pinned via
         * {@link #PIN_RULE_PROPERTY} or chosen randomly from the candidate pool using the current test's seed
         * (so failures reproduce with the same seed).
         *
         * <p>Logs a grep-able {@code optimizer-rule-disabled: ...} line so any failure is attributable to a specific
         * {@code (stage, rule)} pair without re-reading test output.</p>
         */
        @Override
        public TransformedQuery transformQuery(String testId, CsvSpecReader.CsvTestCase testCase) {
            String pragmaValue;
            if (pinnedPragmaValue != null) {
                pragmaValue = pinnedPragmaValue;
            } else {
                // Use the current test's seeded random so the same seed always picks the same rule.
                CandidateRule chosen = candidatePool.get(ESTestCase.random().nextInt(candidatePool.size()));
                pragmaValue = chosen.stage().pragmaKey() + ":" + chosen.ruleName();
            }

            logger.info("optimizer-rule-disabled: testId={} rule={}", testId, pragmaValue);
            LAUNCHED_COUNTS.computeIfAbsent(pragmaValue, k -> new AtomicInteger()).incrementAndGet();

            Settings extraPragmas = Settings.builder().putList(QueryPragmas.DISABLE_OPTIMIZER_RULES.getKey(), pragmaValue).build();
            return new TransformedQuery(testCase.query, extraPragmas);
        }

        @Override
        public CsvTestUtils.ExpectedResults transformExpectedResults(
            String testId,
            CsvSpecReader.CsvTestCase testCase,
            CsvTestUtils.ExpectedResults expected
        ) {
            // Compare against the original csv-spec expected values (the all-rules baseline).
            // Any mismatch is the signal that the disabled rule is either semantics-breaking (a bug)
            // or mandatory (add MandatoryRule marker after per-rule triage).
            return expected;
        }

        @Override
        public String transformExpectedDocumentsFound(CsvSpecReader.CsvTestCase testCase) {
            // Disabling a pushdown rule (e.g. PushTopNToSource) legitimately changes how many Lucene
            // documents are fetched without affecting result values. documents_found is an I/O
            // characteristic, not a correctness property, so suppress it for the differential suite.
            return null;
        }
    }
}
