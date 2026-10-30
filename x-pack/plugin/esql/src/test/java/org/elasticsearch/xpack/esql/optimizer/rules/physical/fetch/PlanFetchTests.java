/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhaseOutcomes.Decision;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhasePolicy.Outcome;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.ReductionPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.compute.operator.topn.TopNOperator.InputOrdering.SORTED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;

public class PlanFetchTests extends ESTestCase {

    private static final String DEFAULT_LIMIT_WARNING = "No limit defined, adding default limit of [1000]";

    private static final String BASIC =
        "FROM employees | WHERE salary > 1000 | SORT hire_date DESC | LIMIT 10 | KEEP first_name, emp_no, hire_date";

    /**
     * The data nodes send the documents and the sort key, and the coordinator fetches the other columns for the
     * winners.
     */
    public void testTopN() {
        Planned planned = plan(BASIC);
        assertThat(planned.decisions(), contains(new Decision(Outcome.APPLIED, null)));

        FetchExec fetch = single(planned.plan(), FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("emp_no", "first_name")));
        assertThat(fetch.indexPattern(), equalTo("employees"));
        assertTrue(EsQueryExec.isDocAttribute(fetch.doc()));

        TopNExec coordinatorCut = as(fetch.child(), TopNExec.class);
        assertThat(coordinatorCut.inputOrdering(), equalTo(SORTED));
        ExchangeExec exchange = as(coordinatorCut.child(), ExchangeExec.class);
        assertThat(names(exchange.output()), equalTo(List.of("_doc", "hire_date")));
        assertThat(exchange.output().getFirst(), equalTo(fetch.doc()));

        FragmentExec fragment = as(exchange.child(), FragmentExec.class);
        Project fragmentRoot = as(fragment.fragment(), Project.class);
        assertThat(fragmentRoot.output(), equalTo(exchange.output()));
        EsRelation relation = fragment.fragment().collect(EsRelation.class).getFirst();
        assertThat(relation.output().getFirst(), equalTo(fetch.doc()));

        // the cut now carries narrow rows: the documents and the sort key, plus what any TopN of _doc reserves to load them
        assertThat(
            coordinatorCut.estimatedRowSize(),
            lessThanOrEqualTo(
                DataType.DOC_DATA_TYPE.estimatedSize() + DocVector.SHARD_SEGMENT_DOC_MAP_PER_ROW_OVERHEAD + DataType.DATETIME
                    .estimatedSize()
            )
        );
    }

    /** The cut reads the sort key, so it crosses the exchange even when the query does not return it. */
    public void testSortKeyNotReturned() {
        FetchExec fetch = single(plan("FROM employees | SORT hire_date | LIMIT 10 | KEEP first_name").plan(), FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "hire_date")));
    }

    /** Without a sort, only the documents cross the exchange. */
    public void testLimit() {
        FetchExec fetch = single(plan("FROM employees | LIMIT 10 | KEEP first_name, emp_no").plan(), FetchExec.class);
        LimitExec coordinatorCut = as(fetch.child(), LimitExec.class);
        assertThat(coordinatorCut.child(), instanceOf(ExchangeExec.class));
        assertThat(names(coordinatorCut.output()), equalTo(List.of("_doc")));
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("emp_no", "first_name")));
    }

    /** The default LIMIT is a cut too: a plain FROM fetches the rows it returns after the coordinator picked them. */
    public void testDefaultLimit() {
        FetchExec fetch = single(plan("FROM employees | KEEP first_name").plan(), FetchExec.class);
        LimitExec coordinatorCut = as(fetch.child(), LimitExec.class);
        assertThat(names(coordinatorCut.output()), equalTo(List.of("_doc")));
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertWarnings(DEFAULT_LIMIT_WARNING);
    }

    /** Without KEEP the query still returns the columns it returned before, in the same order, and never the documents. */
    public void testWithoutKeep() {
        String query = "FROM employees | SORT emp_no | LIMIT 5";
        PhysicalPlan eager = plan(query, EsqlFlags.withFetchPhase(false)).plan();
        PhysicalPlan fetched = plan(query).plan();
        ProjectExec restore = as(fetched, ProjectExec.class);
        assertThat(restore.child(), instanceOf(FetchExec.class));
        assertThat(namesAndTypes(fetched.output()), equalTo(namesAndTypes(eager.output())));
    }

    /** A value computed before the cut crosses as a value, its inputs stay on the data node. */
    public void testEvalBeforeTheCut() {
        FetchExec fetch = single(
            plan("FROM employees | EVAL raise = salary * 2 | SORT hire_date | LIMIT 10 | KEEP raise, first_name").plan(),
            FetchExec.class
        );
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "hire_date", "raise")));
    }

    /**
     * The data node loads {@code emp_no} to compute the sort key, so it crosses as a value instead of being read a second
     * time. With nothing else to fetch, the plan stays eager.
     */
    public void testColumnOfAComputedSortKeyIsNotFetched() {
        assertDeclined(plan("FROM employees | SORT emp_no + 1 | LIMIT 10 | KEEP emp_no"), Outcome.INELIGIBLE_NO_DEFERRABLE_FIELDS);

        FetchExec fetch = single(plan("FROM employees | SORT emp_no + 1 | LIMIT 10 | KEEP emp_no, first_name").plan(), FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(fetch.child().output().stream().map(Attribute::name).toList(), hasItem("emp_no"));
    }

    /** WHERE, EVAL and RENAME after the cut run on the coordinator, over the fetched columns. */
    public void testWhereEvalAndRenameAfterTheCut() {
        PhysicalPlan plan = plan(
            "FROM employees | SORT hire_date | LIMIT 10 | WHERE salary > 1000 | EVAL monthly = salary / 12 "
                + "| RENAME first_name AS name | KEEP name, monthly"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name", "salary")));
    }

    /** {@code _score} comes from the query, not from the document, so it crosses the exchange. The rest is fetched. */
    public void testScoreStaysEager() {
        FetchExec fetch = single(
            plan("FROM employees METADATA _score | WHERE first_name == \"x\" | SORT _score DESC | LIMIT 10 | KEEP first_name, _score")
                .plan(),
            FetchExec.class
        );
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "_score")));
    }

    public void testSourceIsFetched() {
        FetchExec fetch = single(plan("FROM employees METADATA _source | SORT emp_no | LIMIT 3 | KEEP _source").plan(), FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("_source")));
    }

    /** The extraction preference applies to the data drivers. The fetch side always loads the plain values. */
    public void testExtractionPreferenceDoesNotDisableTheFetch() {
        Configuration configuration = configuration(
            Settings.builder().put(QueryPragmas.FIELD_EXTRACT_PREFERENCE.getKey(), MappedFieldType.FieldExtractPreference.STORED).build()
        );
        Planned planned = plan(BASIC, EsqlFlags.withFetchPhase(true), configuration, TransportVersion.current());
        assertThat(planned.decisions(), contains(new Decision(Outcome.APPLIED, null)));
        single(planned.plan(), FetchExec.class);
    }

    /**
     * The coordinator splits the plan at the exchange. The data nodes plan their side like any other: node-level late
     * materialization cuts the rows of the node's shards once more, and the documents leave the node with the sort key.
     */
    public void testSplits() {
        Configuration configuration = configuration(Settings.EMPTY);
        PhysicalPlan plan = plan(BASIC, EsqlFlags.withFetchPhase(true), configuration, TransportVersion.current()).plan();
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndData = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(plan, configuration);
        assertThat(coordinatorAndData.v1().collect(FetchExec.class), hasSize(1));
        ExchangeSinkExec dataNodePlan = as(coordinatorAndData.v2(), ExchangeSinkExec.class);
        assertThat(names(dataNodePlan.output()), equalTo(List.of("_doc", "hire_date")));

        ReductionPlan reduction = ComputeService.reductionPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            configuration,
            configuration.newFoldContext(),
            dataNodePlan,
            true,
            true,
            null
        );
        TopNExec nodeCut = single(reduction.nodeReducePlan(), TopNExec.class);
        assertThat(nodeCut.child(), instanceOf(ExchangeSourceExec.class));
        assertThat(names(reduction.nodeReducePlan().output()), equalTo(List.of("_doc", "hire_date")));
        assertThat(names(reduction.dataNodePlan().output()), equalTo(List.of("_doc", "hire_date")));
    }

    public void testSwitches() {
        assertDeclined(plan(BASIC, EsqlFlags.withFetchPhase(false)), Outcome.DISABLED_SETTING);
        assertDeclined(
            plan(BASIC, EsqlFlags.withFetchPhase(true), configuration(fetchPhasePragma(false)), TransportVersion.current()),
            Outcome.DISABLED_PRAGMA
        );
        assertThat(
            plan(BASIC, EsqlFlags.withFetchPhase(false), configuration(fetchPhasePragma(true)), TransportVersion.current()).decisions(),
            contains(new Decision(Outcome.APPLIED, null))
        );
        TransportVersion old = TransportVersionUtils.getPreviousVersion(FetchPhasePolicy.PLANNER_MINIMUM);
        assertDeclined(plan(BASIC, EsqlFlags.withFetchPhase(true), configuration(Settings.EMPTY), old), Outcome.MIXED_VERSION_FALLBACK);
        EsqlFlags unavailable = new EsqlFlags(
            true,
            -1,
            20,
            5,
            8,
            1000,
            500,
            EsqlFlags.ESQL_METRICS_INFO_MAX_TIME_RANGE.getDefault(Settings.EMPTY),
            EsqlFlags.FetchPhaseMode.UNAVAILABLE
        );
        assertDeclined(
            plan(BASIC, unavailable, configuration(fetchPhasePragma(true)), TransportVersion.current()),
            Outcome.DISABLED_FEATURE_FLAG
        );
    }

    /** A plan the rule declines is returned as it came in, so the query plans exactly as without the fetch phase. */
    public void testDeclinedPlansAreUntouched() {
        PhysicalPlan eager = plan("FROM employees | SORT hire_date | LIMIT 10 | STATS m = MAX(salary)", EsqlFlags.withFetchPhase(false))
            .plan();
        FetchPhaseOutcomes outcomes = new FetchPhaseOutcomes();
        PhysicalPlan rewritten = new PlanFetch(outcomes).apply(eager, context(EsqlFlags.withFetchPhase(true)));
        assertThat(rewritten, sameInstance(eager));
        assertThat(outcomes.all().getFirst().outcome(), equalTo(Outcome.INELIGIBLE_SHAPE));
    }

    /** Running the rule on its own output changes nothing: the fetch now runs after the cut. */
    public void testRunningTwiceIsANoop() {
        PhysicalPlan fetched = plan(BASIC).plan();
        FetchPhaseOutcomes outcomes = new FetchPhaseOutcomes();
        assertThat(new PlanFetch(outcomes).apply(fetched, context(EsqlFlags.withFetchPhase(true))), sameInstance(fetched));
        assertThat(outcomes.all(), contains(new Decision(Outcome.INELIGIBLE_SHAPE, "[FetchExec] runs after the cut")));
    }

    public void testIneligibleShapes() {
        // the optimizer puts the default LIMIT on top of the aggregation
        assertIneligible("FROM employees | SORT hire_date | LIMIT 10 | STATS m = MAX(salary)", "[AggregateExec] runs after the cut");
        assertIneligible("FROM employees | STATS m = MAX(salary) BY hire_date | SORT m | LIMIT 10", "aggregation");
        assertIneligible(
            "FROM employees | SORT salary | LIMIT 100 | SORT hire_date | LIMIT 10 | KEEP first_name, hire_date",
            "[TopNExec] runs after the cut"
        );
        assertIneligible(
            "FROM employees | MV_EXPAND languages | SORT hire_date | LIMIT 10 | KEEP first_name, languages",
            "[MvExpand] runs before the cut"
        );
    }

    public void testNoExchange() {
        assertIneligible("ROW x = 1", "no exchange");
        assertWarnings(DEFAULT_LIMIT_WARNING);
    }

    public void testNothingToDefer() {
        assertDeclined(plan("FROM employees | SORT hire_date | LIMIT 10 | KEEP hire_date"), Outcome.INELIGIBLE_NO_DEFERRABLE_FIELDS);
    }

    /**
     * A field that may be unmapped is read through a rule that runs on the data node and that the fetch side does not
     * run, so it keeps crossing the exchange while the other columns are fetched.
     */
    public void testPotentiallyUnmappedFieldStaysEager() {
        Attribute unmapped = new FieldAttribute(Source.EMPTY, "unmapped_kw", new PotentiallyUnmappedKeywordEsField("unmapped_kw"));
        Attribute mapped = field("first_name", DataType.KEYWORD);
        PhysicalPlan rewritten = applyTo(topNPlan(List.of(mapped, unmapped), Map.of("", List.of("idx"))));
        FetchExec fetch = single(rewritten, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "sort", "unmapped_kw")));
    }

    /** Rows from a remote cluster cannot be fetched yet, the plan stays eager. */
    public void testRemoteClusterStaysEager() {
        FetchPhaseOutcomes outcomes = new FetchPhaseOutcomes();
        PhysicalPlan plan = topNPlan(List.of(field("first_name", DataType.KEYWORD)), Map.of("remote", List.of("idx")));
        assertThat(new PlanFetch(outcomes).apply(plan, context(EsqlFlags.withFetchPhase(true))), sameInstance(plan));
        assertThat(outcomes.all().getFirst().outcome(), equalTo(Outcome.INELIGIBLE_REMOTE_CLUSTER));
    }

    /** A local TopN only cuts the rows of one node, so it cannot be the data node half of the coordinator cut. */
    public void testLocalCutDoesNotMirrorTheCoordinatorCut() {
        PhysicalPlan plan = topNPlan(List.of(field("first_name", DataType.KEYWORD)), Map.of("", List.of("idx")));
        PhysicalPlan localCut = plan.transformDown(
            FragmentExec.class,
            fragment -> fragment.withFragment(fragment.fragment().transformDown(TopN.class, topN -> topN.withLocal(true)))
        );
        FetchPhaseOutcomes outcomes = new FetchPhaseOutcomes();
        assertThat(new PlanFetch(outcomes).apply(localCut, context(EsqlFlags.withFetchPhase(true))), sameInstance(localCut));
        assertThat(outcomes.all(), hasSize(1));
        assertThat(outcomes.all().getFirst().outcome(), equalTo(Outcome.INELIGIBLE_SHAPE));
        assertThat(outcomes.all().getFirst().reason(), containsString("does not repeat the cut that ends the fragment"));
        assertThat("the same plan with a pipeline breaking cut gets the fetch phase", applyTo(plan).collect(FetchExec.class), hasSize(1));
    }

    private static void assertIneligible(String query, String reason) {
        List<Decision> decisions = plan(query).decisions();
        assertThat(query, decisions, hasSize(1));
        assertThat(query, decisions.getFirst().outcome(), equalTo(Outcome.INELIGIBLE_SHAPE));
        assertThat(query, decisions.getFirst().reason(), containsString(reason));
    }

    private static void assertDeclined(Planned planned, Outcome outcome) {
        assertThat(planned.decisions(), hasSize(1));
        assertThat(planned.decisions().getFirst().outcome(), equalTo(outcome));
        assertThat(planned.plan().collect(FetchExec.class), empty());
    }

    private record Planned(PhysicalPlan plan, List<Decision> decisions) {}

    private static Planned plan(String query) {
        return plan(query, EsqlFlags.withFetchPhase(true));
    }

    private static Planned plan(String query, EsqlFlags flags) {
        return plan(query, flags, configuration(Settings.EMPTY), TransportVersion.current());
    }

    private static Planned plan(String query, EsqlFlags flags, Configuration configuration, TransportVersion minimumVersion) {
        Analyzer analyzer = EsqlTestUtils.analyzer()
            .addIndex(EsIndexGenerator.esIndex("employees", mapping(), Map.of("employees", IndexMode.STANDARD)))
            .minimumTransportVersion(minimumVersion)
            .buildAnalyzer();
        TestPlannerOptimizer optimizer = new TestPlannerOptimizer(configuration, analyzer, flags);
        PhysicalPlan plan = optimizer.distributedPlan(query);
        return new Planned(plan, optimizer.fetchPhaseOutcomes().all());
    }

    private static Map<String, EsField> mapping() {
        return Map.of(
            "emp_no",
            new EsField("emp_no", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "first_name",
            new KeywordEsField("first_name", Map.of(), true, 32766, false, false, EsField.TimeSeriesFieldType.NONE),
            "hire_date",
            new EsField("hire_date", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "languages",
            new EsField("languages", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "salary",
            new EsField("salary", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static Settings fetchPhasePragma(boolean enabled) {
        return Settings.builder().put(QueryPragmas.FETCH_PHASE.getKey(), enabled).build();
    }

    private static Configuration configuration(Settings pragmas) {
        return EsqlTestUtils.configuration(new QueryPragmas(pragmas));
    }

    private static <T extends PhysicalPlan> T single(PhysicalPlan plan, Class<T> type) {
        List<T> found = plan.collect(type);
        assertThat(plan.toString(), found, hasSize(1));
        return found.getFirst();
    }

    private static <T> T as(Object value, Class<T> type) {
        assertThat(value, instanceOf(type));
        return type.cast(value);
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }

    private static List<String> namesAndTypes(List<Attribute> attributes) {
        return attributes.stream().map(a -> a.name() + ":" + a.dataType()).toList();
    }

    private static PhysicalOptimizerContext context(EsqlFlags flags) {
        return new PhysicalOptimizerContext(configuration(Settings.EMPTY), TransportVersion.current(), flags);
    }

    private static PhysicalPlan applyTo(PhysicalPlan plan) {
        return new PlanFetch(new FetchPhaseOutcomes()).apply(plan, context(EsqlFlags.withFetchPhase(true)));
    }

    /**
     * The plan {@code ProjectAwayColumns} leaves for {@code FROM idx | SORT sort | LIMIT 10 | KEEP returned...}, built by
     * hand for fields and relations the analyzer cannot produce in a unit test.
     */
    private static PhysicalPlan topNPlan(List<Attribute> returned, Map<String, List<String>> concreteIndices) {
        Attribute sort = field("sort", DataType.LONG);
        List<Order> order = List.of(new Order(Source.EMPTY, sort, Order.OrderDirection.ASC, Order.NullsPosition.LAST));
        List<Attribute> relationOutput = new ArrayList<>(returned);
        relationOutput.add(sort);
        EsRelation relation = new EsRelation(
            Source.EMPTY,
            "idx",
            IndexMode.STANDARD,
            Map.of("", List.of("idx")),
            concreteIndices,
            Map.of("idx", new IndexProperties(IndexMode.STANDARD, 0)),
            relationOutput
        );
        TopN topN = new TopN(Source.EMPTY, relation, order, EsqlTestUtils.of(10), false);
        List<Attribute> exchangeOutput = new ArrayList<>(returned);
        exchangeOutput.add(1, sort);
        Project fragmentRoot = new Project(Source.EMPTY, topN, exchangeOutput);
        ExchangeExec exchange = new ExchangeExec(Source.EMPTY, fragmentRoot.output(), false, new FragmentExec(fragmentRoot));
        return new ProjectExec(Source.EMPTY, new TopNExec(Source.EMPTY, exchange, order, EsqlTestUtils.of(10), null), returned);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}
