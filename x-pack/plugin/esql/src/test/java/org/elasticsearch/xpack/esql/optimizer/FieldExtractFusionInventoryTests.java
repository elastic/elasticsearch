/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression.PushedBlockLoaderExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FieldExtract;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.junit.Before;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;

/**
 * Inventory / audit harness for {@code field_extract} block-loader <em>fusion</em>.
 * <p>
 *     "Fusion" is the fast path for {@code field_extract(<flattened root>, "<key>")}: instead of
 *     materializing the whole flattened JSON blob per row and re-parsing it in a compute-engine
 *     evaluator, the call is folded into the field load so the keyed sub-field's doc values are read
 *     directly. The decision is made during local physical planning by
 *     {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushExpressionsToFieldLoad},
 *     which replaces a pushable {@link FieldExtract} with a synthetic {@link FieldAttribute} backed by
 *     a {@link FunctionEsField} carrying an {@code ExtractFlattenedSubfieldConfig}.
 * </p>
 * <p>
 *     This test plans representative query shapes offline (no cluster) with
 *     {@link TestPlannerOptimizer} over a flattened mapping, then walks the optimized physical plan to
 *     count two things:
 * </p>
 * <ul>
 *     <li><b>fused</b> loads: {@link FieldAttribute}s whose {@link FieldAttribute#field()} is a
 *     {@link FunctionEsField} with function {@link BlockLoaderFunctionConfig.Function#EXTRACT_FLATTENED_SUBFIELD};</li>
 *     <li><b>fallback</b> loads: {@link FieldExtract} expressions that survived the rule (i.e. still run
 *     the per-row evaluator), each bucketed by <em>why</em> it did not fuse.</li>
 * </ul>
 * <p>
 *     The {@code EVAL} shapes carry a trailing {@code | SORT id | LIMIT 10}. Fusion is a local
 *     (data-node) physical optimization, so an {@code EVAL} must sit <em>below</em> the exchange boundary
 *     to be eligible; a bare {@code FROM ... | EVAL ... | KEEP} leaves the {@code EVAL} on the coordinator
 *     side where the rule never runs. The {@code SORT}/{@code LIMIT} pushes it into the data-node fragment,
 *     mirroring {@code PushExpressionsToFieldLoadTests}. A {@code WHERE} predicate is already data-node local.
 * </p>
 * <p>
 *     The {@link #classify} reasons mirror the gates in {@code PushExpressionsToFieldLoad#transformExpression}
 *     one-for-one, so a residual {@code field_extract} is attributed to the exact gate that rejected it.
 *     This is the offline sibling of the corpus-driven {@code CsvFlattenedKeywordIT}: it answers "which
 *     query shapes still fall back, and why" without needing to run the whole CSV corpus on a cluster, and
 *     gives a place to add regression coverage as more shapes become fusible on the road to GA.
 * </p>
 * <p>
 *     {@link Fusion#UNION_TYPE} is defined for completeness but is not reachable through {@code field_extract}:
 *     the function requires its first argument to resolve to a {@code FLATTENED} {@link FieldAttribute}, and a
 *     field with conflicting mappings across indices resolves to a union type instead, so the call never
 *     analyzes. The gate still exists in the rule because it guards <em>all</em> block-loader expressions.
 * </p>
 */
public class FieldExtractFusionInventoryTests extends AbstractLocalPhysicalPlanOptimizerTests {

    /**
     * Plain flattened root ({@code data}) plus two keyword fields ({@code id}, {@code key}). {@code data}
     * has no mapped sub-fields, so every literal key is an unmapped keyed sub-field and is fusible; {@code key}
     * gives us a non-foldable (column) path to exercise the dynamic-key fallback.
     */
    private static final String FLATTENED_MAPPING = "mapping-flattened_keyed.json";

    /**
     * Why a {@code field_extract} did not fuse. Each non-{@link #FUSED} value corresponds to one gate in
     * {@code PushExpressionsToFieldLoad#transformExpression}, in the order they are checked.
     */
    enum Fusion {
        /** Fused into the keyed sub-field doc-values loader (the fast path). */
        FUSED,
        /** Path is not a foldable literal, so no keyed loader can be built at plan time. */
        DYNAMIC_KEY,
        /** First argument is not a real {@code FLATTENED} {@link FieldAttribute} (e.g. an alias/expression). */
        NON_FLATTENED_INPUT,
        /** The flattened root is a {@link UnionTypeEsField} (conflicting mappings across indices). */
        UNION_TYPE,
        /** Field lineage does not trace to exactly one non-LOOKUP source (e.g. above a {@code LOOKUP JOIN}). */
        ABOVE_JOIN_OR_MULTISOURCE,
        /**
         * The field type rejected the loader config for this key (a mapped sub-field, or no doc values),
         * so reading it via the keyed channel would diverge from the keyword evaluator.
         */
        UNSUPPORTED_LOADER_CONFIG
    }

    /** Outcome of walking one optimized plan: how many loads fused, and the fallback bucket histogram. */
    record Inventory(int fused, Map<Fusion, Integer> fallbackByReason) {
        int fallback() {
            return fallbackByReason.values().stream().mapToInt(Integer::intValue).sum();
        }
    }

    private TestPlannerOptimizer flattenedPlanner;

    public FieldExtractFusionInventoryTests(String name, Configuration config) {
        super(name, config);
    }

    @Before
    public void setUpFlattenedPlanner() {
        assumeTrue("field_extract must be part of this build for the plans to analyze", FieldExtract.isFnFieldExtractCapabilityMet());
        flattenedPlanner = new TestPlannerOptimizer(config, makeAnalyzer(FLATTENED_MAPPING));
    }

    // ---- shape matrix -------------------------------------------------------------------------------

    public void testConstantKeyFusesOnFlattenedRoot() {
        Inventory inv = inventory(
            "FROM test | EVAL x = field_extract(data, \"host.name\") | SORT id | LIMIT 10 | KEEP x",
            EsqlTestUtils.TEST_SEARCH_STATS
        );
        assertEquals("a literal key on a plain flattened root must fuse", 1, inv.fused());
        assertEquals("nothing should fall back", 0, inv.fallback());
    }

    public void testConsumingFunctionDoesNotBlockFusion() {
        // The field_extract fuses even though its result is immediately consumed by another scalar function;
        // fusion is about how the value is loaded, not what happens to it afterwards.
        Inventory inv = inventory(
            "FROM test | EVAL x = TO_UPPER(field_extract(data, \"host.name\")) | SORT id | LIMIT 10 | KEEP x",
            EsqlTestUtils.TEST_SEARCH_STATS
        );
        assertEquals(1, inv.fused());
        assertEquals(0, inv.fallback());
    }

    public void testMultipleDistinctKeysEachFuse() {
        Inventory inv = inventory(
            "FROM test | EVAL a = field_extract(data, \"k1\"), b = field_extract(data, \"k2\") | SORT id | LIMIT 10 | KEEP a, b",
            EsqlTestUtils.TEST_SEARCH_STATS
        );
        assertEquals("each distinct key becomes its own fused keyed load", 2, inv.fused());
        assertEquals(0, inv.fallback());
    }

    public void testFilterPredicateFuses() {
        // field_extract in a WHERE predicate still needs the extracted keyword column (the pushed Lucene
        // query is a RECHECK candidate), so the load fuses rather than falling back to the per-row evaluator.
        Inventory inv = inventory(
            "FROM test | WHERE field_extract(data, \"host.name\") == \"v\" | KEEP id",
            EsqlTestUtils.TEST_SEARCH_STATS
        );
        assertEquals(1, inv.fused());
        assertEquals(0, inv.fallback());
    }

    public void testDynamicKeyFallsBack() {
        // A column path (the keyword field `key`) is not foldable, so no keyed loader can be built.
        Inventory inv = inventory(
            "FROM test | EVAL x = field_extract(data, key) | SORT id | LIMIT 10 | KEEP x",
            EsqlTestUtils.TEST_SEARCH_STATS
        );
        assertEquals("a dynamic key cannot fuse", 0, inv.fused());
        assertEquals(Map.of(Fusion.DYNAMIC_KEY, 1), inv.fallbackByReason());
    }

    public void testAboveUnionDegradesToNonFieldAttribute() {
        assumeTrue("subqueries must be enabled", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("subqueries must be enabled", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled());
        // Above a two-source union the flattened root is no longer a direct FieldAttribute (it is the union's
        // merged reference), so field_extract cannot fuse. This is the union-shaped face of "multi-source":
        // the attribute degrades before the Primaries lineage check even applies.
        Inventory inv = inventory(
            "FROM test, (FROM test | KEEP data, id) | EVAL x = field_extract(data, \"host.name\") | KEEP x",
            EsqlTestUtils.TEST_SEARCH_STATS
        );
        assertEquals(0, inv.fused());
        assertEquals(Map.of(Fusion.NON_FLATTENED_INPUT, 1), inv.fallbackByReason());
    }

    public void testAboveLookupJoinFallsBack() {
        // field_extract sits above a LOOKUP JOIN, referencing the flattened root from the left (main) side.
        // The root stays a real FieldAttribute, so the call is pushable in isolation, but its lineage now
        // traces to two sources (main + lookup). The Primaries check forbids the push: the ABOVE_JOIN gate.
        TestPlannerOptimizer joinPlanner = new TestPlannerOptimizer(config, joinAnalyzer());
        Inventory inv = inventory(joinPlanner, """
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | EVAL x = field_extract(data, "host.name")
            | SORT language_code
            | LIMIT 10
            | KEEP x
            """, EsqlTestUtils.TEST_SEARCH_STATS);
        assertEquals(0, inv.fused());
        assertEquals(Map.of(Fusion.ABOVE_JOIN_OR_MULTISOURCE, 1), inv.fallbackByReason());
    }

    public void testUnsupportedLoaderConfigFallsBack() {
        // Stats that reject every loader config stand in for a mapped sub-field / a field without doc values:
        // the key is foldable and the root is flattened, but the field type declines the keyed load.
        Inventory inv = inventory(
            "FROM test | EVAL x = field_extract(data, \"host.name\") | SORT id | LIMIT 10 | KEEP x",
            new EsqlTestUtils.TestSearchStats(false)
        );
        assertEquals(0, inv.fused());
        assertEquals(Map.of(Fusion.UNSUPPORTED_LOADER_CONFIG, 1), inv.fallbackByReason());
    }

    // ---- corpus-style summary ------------------------------------------------------------------------

    /**
     * Aggregates a spread of shapes into one fused-vs-fallback-by-reason report and logs it. This is the
     * offline shape of the summary a corpus-wide dry run produces: a single-glance tally of how many loads
     * fuse and, for those that don't, which gate stopped them. Extend {@link #corpus()} as shapes are added.
     * <p>
     *     A faithful whole-corpus version belongs in the {@code internalClusterTest} module, where
     *     {@code CsvFlattenedKeywordIT}'s {@code AstKeywordFieldRewriter} and per-dataset flattened mappings
     *     already exist: feed each rewritten CSV query through the same {@link #inventory} walk instead of
     *     executing it on a cluster, and aggregate identically.
     * </p>
     */
    public void testFusionInventorySummary() {
        Inventory total = aggregate(corpus());

        StringBuilder report = new StringBuilder("field_extract fusion inventory: ").append(total.fused()).append(" fused");
        total.fallbackByReason().forEach((reason, count) -> report.append(", ").append(reason).append('=').append(count));
        logger.info(report.toString());

        // Sanity checks on the aggregate: the fusible shapes fused, and every non-fusible shape landed in a bucket.
        assertEquals("constant + two-key fusions", 3, total.fused());
        assertEquals(
            Map.of(Fusion.DYNAMIC_KEY, 1, Fusion.UNSUPPORTED_LOADER_CONFIG, 1, Fusion.ABOVE_JOIN_OR_MULTISOURCE, 1),
            total.fallbackByReason()
        );
    }

    /** The curated mini-corpus behind {@link #testFusionInventorySummary}; capability-gated shapes are excluded. */
    private Inventory aggregate(List<Inventory> runs) {
        int fused = 0;
        Map<Fusion, Integer> byReason = new EnumMap<>(Fusion.class);
        for (Inventory run : runs) {
            fused += run.fused();
            run.fallbackByReason().forEach((reason, count) -> byReason.merge(reason, count, Integer::sum));
        }
        return new Inventory(fused, byReason);
    }

    private List<Inventory> corpus() {
        SearchStats supported = EsqlTestUtils.TEST_SEARCH_STATS;
        TestPlannerOptimizer joinPlanner = new TestPlannerOptimizer(config, joinAnalyzer());
        return List.of(
            inventory("FROM test | EVAL x = field_extract(data, \"host.name\") | SORT id | LIMIT 10 | KEEP x", supported),
            inventory(
                "FROM test | EVAL a = field_extract(data, \"k1\"), b = field_extract(data, \"k2\") | SORT id | LIMIT 10 | KEEP a, b",
                supported
            ),
            inventory("FROM test | EVAL x = field_extract(data, key) | SORT id | LIMIT 10 | KEEP x", supported),
            inventory(
                "FROM test | EVAL x = field_extract(data, \"host.name\") | SORT id | LIMIT 10 | KEEP x",
                new EsqlTestUtils.TestSearchStats(false)
            ),
            inventory(joinPlanner, """
                FROM test
                | RENAME languages AS language_code
                | LOOKUP JOIN languages_lookup ON language_code
                | EVAL x = field_extract(data, "host.name")
                | SORT language_code
                | LIMIT 10
                | KEEP x
                """, supported)
        );
    }

    // ---- walker -------------------------------------------------------------------------------------

    private Inventory inventory(String esql, SearchStats stats) {
        return inventory(flattenedPlanner, esql, stats);
    }

    private Inventory inventory(TestPlannerOptimizer planner, String esql, SearchStats stats) {
        PhysicalPlan plan = planner.plan(esql, stats);

        // A node's forEachExpressionDown only walks that node's own expressions, so descend the physical
        // tree first and inspect each node's expressions (the same idiom PushExpressionsToFieldLoadTests uses).
        Set<NameId> fused = new HashSet<>();
        Map<Fusion, Integer> fallbackByReason = new EnumMap<>(Fusion.class);
        Set<FieldExtract> seen = Collections.newSetFromMap(new IdentityHashMap<>());

        plan.forEachDown(PhysicalPlan.class, node -> {
            // Fused loads are synthetic FieldAttributes backed by a FunctionEsField for the flattened
            // sub-field extraction. The same attribute is referenced from several nodes (Eval,
            // FieldExtractExec, Project), so dedup by NameId: it is stable per logical attribute, avoiding
            // both instance double-counting and the name collisions a plain name set would silently collapse.
            node.forEachExpressionDown(FieldAttribute.class, fa -> {
                if (fa.field() instanceof FunctionEsField fe
                    && fe.functionConfig() != null
                    && fe.functionConfig().function() == BlockLoaderFunctionConfig.Function.EXTRACT_FLATTENED_SUBFIELD) {
                    fused.add(fa.id());
                }
            });
            // Fallback loads are surviving FieldExtract expressions. Dedup by node identity so a call is
            // counted once, then attribute each to the gate that rejected it.
            node.forEachExpressionDown(FieldExtract.class, fx -> {
                if (seen.add(fx)) {
                    fallbackByReason.merge(classify(fx, stats), 1, Integer::sum);
                }
            });
        });

        return new Inventory(fused.size(), fallbackByReason);
    }

    /**
     * Re-derives the fusion decision for a residual {@link FieldExtract}, mirroring the gate order in
     * {@code PushExpressionsToFieldLoad#transformExpression}. If the call is pushable in isolation yet still
     * survived in the plan, the only remaining reason is lineage (it sits above a join / multi-source), which
     * is the {@link Fusion#ABOVE_JOIN_OR_MULTISOURCE} fall-through.
     */
    private Fusion classify(FieldExtract fx, SearchStats stats) {
        PushedBlockLoaderExpression fuse = fx.tryPushToFieldLoading(stats);
        if (fuse == null) {
            // tryPushToFieldLoading returns null either because the path is not foldable, or because the
            // field is not a real FLATTENED FieldAttribute. The path argument is the second child.
            Expression pathArg = fx.children().get(1);
            return pathArg.foldable() ? Fusion.NON_FLATTENED_INPUT : Fusion.DYNAMIC_KEY;
        }
        if (fuse.field().field() instanceof UnionTypeEsField) {
            return Fusion.UNION_TYPE;
        }
        // Mirror PushExpressionsToFieldLoad, which passes the configured pragma preference (not a hardcoded NONE),
        // so the audit stays aligned if the default field-extract preference ever changes.
        if (stats.supportsLoaderConfig(fuse.field().fieldName(), fuse.config(), config.pragmas().fieldExtractPreference()) == false) {
            return Fusion.UNSUPPORTED_LOADER_CONFIG;
        }
        return Fusion.ABOVE_JOIN_OR_MULTISOURCE;
    }

    /**
     * Analyzer over {@code mapping-basic.json} plus a synthetic flattened {@code data} field and the shared
     * lookup resolution, so a {@code LOOKUP JOIN} shape can put a fusible {@code field_extract(data, ...)}
     * above the join.
     */
    private Analyzer joinAnalyzer() {
        Map<String, EsField> mapping = new LinkedHashMap<>(loadMapping("mapping-basic.json"));
        mapping.put("data", new EsField("data", DataType.FLATTENED, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        EsIndex test = EsIndexGenerator.esIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        return new Analyzer(
            testAnalyzerContext(
                config,
                TEST_FUNCTION_REGISTRY,
                indexResolutions(test),
                defaultLookupResolution(),
                new EnrichResolution(),
                emptyInferenceResolution()
            ),
            new Verifier(new Metrics(TEST_FUNCTION_REGISTRY, true, true), new XPackLicenseState(() -> 0L))
        );
    }
}
