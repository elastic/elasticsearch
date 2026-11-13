/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.fetch;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTests;
import org.elasticsearch.xpack.esql.analysis.rules.ResolveHighlightIndexKey;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Fetch;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.HighlightExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags.FetchPhaseMode;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.ReductionPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.compute.operator.topn.TopNOperator.InputOrdering.SORTED;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;

public class PlanFetchTests extends ESTestCase {

    private static final String DEFAULT_LIMIT_WARNING = "No limit defined, adding default limit of [1000]";

    private static final EsqlFlags DISABLED = EsqlFlags.withFetchPhase(FetchPhaseMode.DISABLED);
    private static final EsqlFlags AUTO = EsqlFlags.withFetchPhase(FetchPhaseMode.AUTO);
    private static final EsqlFlags ALWAYS = EsqlFlags.withFetchPhase(FetchPhaseMode.ALWAYS);

    private static final String BASIC =
        "FROM employees | WHERE salary > 1000 | SORT hire_date DESC | LIMIT 10 | KEEP first_name, emp_no, hire_date";

    /**
     * The data nodes send the documents and the sort key, and the coordinator fetches the other columns for the
     * winners.
     */
    public void testTopN() {
        FetchExec fetch = single(plan(BASIC), FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("emp_no", "first_name")));
        assertThat(fetch.indexPattern(), equalTo("employees"));
        assertTrue(EsQueryExec.isDocAttribute(fetch.doc()));

        TopNExec coordinatorCut = as(narrowed(fetch).child(), TopNExec.class);
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
        FetchExec fetch = single(plan("FROM employees | SORT hire_date | LIMIT 10 | KEEP first_name"), FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "hire_date")));
    }

    /** Without a sort, only the documents cross the exchange. */
    public void testLimit() {
        FetchExec fetch = single(plan("FROM employees | LIMIT 10 | KEEP first_name, emp_no"), FetchExec.class);
        LimitExec coordinatorCut = as(narrowed(fetch).child(), LimitExec.class);
        assertThat(coordinatorCut.child(), instanceOf(ExchangeExec.class));
        assertThat(names(coordinatorCut.output()), equalTo(List.of("_doc")));
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("emp_no", "first_name")));
    }

    /** The default LIMIT is a cut too: a plain FROM fetches the rows it returns after the coordinator picked them. */
    public void testDefaultLimit() {
        FetchExec fetch = single(plan("FROM employees | KEEP first_name"), FetchExec.class);
        LimitExec coordinatorCut = as(narrowed(fetch).child(), LimitExec.class);
        assertThat(names(coordinatorCut.output()), equalTo(List.of("_doc")));
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertWarnings(DEFAULT_LIMIT_WARNING);
    }

    /** Without KEEP the query still returns the columns it returned before, in the same order, and never the documents. */
    public void testWithoutKeep() {
        String query = "FROM employees | SORT emp_no | LIMIT 5";
        PhysicalPlan eager = plan(query, DISABLED);
        PhysicalPlan fetched = plan(query);
        ProjectExec restore = as(fetched, ProjectExec.class);
        assertThat(restore.child(), instanceOf(FetchExec.class));
        assertThat(namesAndTypes(fetched.output()), equalTo(namesAndTypes(eager.output())));
    }

    /** A value computed before the cut crosses as a value, its inputs stay on the data node. */
    public void testEvalBeforeTheCut() {
        FetchExec fetch = single(
            plan("FROM employees | EVAL raise = salary * 2 | SORT hire_date | LIMIT 10 | KEEP raise, first_name"),
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
        assertEager(plan("FROM employees | SORT emp_no + 1 | LIMIT 10 | KEEP emp_no"));

        FetchExec fetch = single(plan("FROM employees | SORT emp_no + 1 | LIMIT 10 | KEEP emp_no, first_name"), FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(fetch.child().output().stream().map(Attribute::name).toList(), hasItem("emp_no"));
    }

    /** WHERE, EVAL and RENAME after the cut run on the coordinator, over the fetched columns. */
    public void testWhereEvalAndRenameAfterTheCut() {
        PhysicalPlan plan = plan(
            "FROM employees | SORT hire_date | LIMIT 10 | WHERE salary > 1000 | EVAL monthly = salary / 12 "
                + "| RENAME first_name AS name | KEEP name, monthly"
        );
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name", "salary")));
    }

    /**
     * {@code HIGHLIGHT} after the cut runs on the coordinator over the fetched text of the winners, so the fields it
     * highlights are fetched instead of crossing the exchange.
     */
    public void testHighlightAfterTheCutFetchesTheFieldsItHighlights() {
        PhysicalPlan plan = plan(
            books(),
            "FROM books | SORT year | LIMIT 10 | HIGHLIGHT \"ring\" ON title | KEEP book_no, highlight_title"
        );
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("book_no", "title")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "year")));
        assertThat("the highlight reads the fetched text", single(plan, HighlightExec.class).child(), sameInstance(fetch));
    }

    /**
     * The optimizer moves the sort and the limit below a {@code HIGHLIGHT}, so a {@code HIGHLIGHT} written before them
     * still follows the cut and the plan gets the fetch phase.
     */
    public void testHighlightWrittenBeforeTheSortFollowsTheCut() {
        PhysicalPlan plan = plan(
            books(),
            "FROM books | HIGHLIGHT \"ring\" ON title | SORT year | LIMIT 10 | KEEP book_no, highlight_title"
        );
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("book_no", "title")));
        assertThat(single(plan, HighlightExec.class).child(), sameInstance(fetch));
    }

    /**
     * A sort on a generated highlight column keeps {@code HIGHLIGHT} before the cut, on the data nodes. The highlight
     * crosses the exchange as a value and the other columns are fetched.
     */
    public void testHighlightBeforeTheCutRunsOnTheDataNodes() {
        PhysicalPlan plan = plan(
            books(),
            "FROM books | HIGHLIGHT \"ring\" ON title | SORT highlight_title | LIMIT 10 | KEEP book_no, highlight_title"
        );
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("book_no")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "highlight_title")));
    }

    /**
     * When the indices disagree on the analyzer of a highlighted field, {@code HIGHLIGHT} also reads the index of each row.
     * The optimizer copies {@code _index} into the key after the cut, so {@code _index} is fetched with the text, and only
     * the documents and the sort key cross the exchange.
     */
    public void testHighlightFetchesTheIndexOfEachRowWithTheText() {
        TestAnalyzer books = AnalyzerTests.booksWithConflictingTitleAnalyzer().minimumTransportVersion(TransportVersion.current());
        PhysicalPlan plan = plan(
            books,
            "FROM books* | SORT book_no | LIMIT 10 | HIGHLIGHT \"ring\" ON title | KEEP book_no, highlight_title"
        );
        assertThat(single(plan, HighlightExec.class).indexKey().name(), equalTo(ResolveHighlightIndexKey.INDEX_KEY_NAME));
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("title", "_index")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "book_no")));
    }

    /** {@code _score} comes from the query, not from the document, so it crosses the exchange. The rest is fetched. */
    public void testScoreStaysEager() {
        FetchExec fetch = single(
            plan("FROM employees METADATA _score | WHERE first_name == \"x\" | SORT _score DESC | LIMIT 10 | KEEP first_name, _score"),
            FetchExec.class
        );
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "_score")));
    }

    public void testSourceIsFetched() {
        FetchExec fetch = single(plan("FROM employees METADATA _source | SORT emp_no | LIMIT 3 | KEEP _source"), FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("_source")));
    }

    /** The extraction preference applies to the data drivers. The fetch side always loads the plain values. */
    public void testExtractionPreferenceDoesNotDisableTheFetch() {
        Configuration configuration = configuration(
            Settings.builder().put(QueryPragmas.FIELD_EXTRACT_PREFERENCE.getKey(), MappedFieldType.FieldExtractPreference.STORED).build()
        );
        single(plan(BASIC, ALWAYS, configuration, TransportVersion.current()), FetchExec.class);
    }

    /**
     * The coordinator splits the plan at the exchange. The data nodes plan their side like any other: node-level late
     * materialization cuts the rows of the node's shards once more, and the documents leave the node with the sort key.
     */
    public void testSplits() {
        Configuration configuration = configuration(Settings.EMPTY);
        PhysicalPlan plan = plan(BASIC, ALWAYS, configuration, TransportVersion.current());
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

    /** Nothing turns the rule on but {@link EsqlFlags#fetchPhaseMode()}. */
    public void testRunsOnlyWhenTheFlagsEnableIt() {
        assertEager(plan(BASIC, DISABLED));
        single(plan(BASIC, ALWAYS), FetchExec.class);
    }

    /**
     * With {@code AUTO}, a {@code TopN} that defers {@code _source} gets the fetch phase. The round trip is paid anyway,
     * so the other columns the cut doesn't read are fetched with it.
     */
    public void testAutoFetchesATopNThatDefersTheSource() {
        FetchExec fetch = single(
            plan("FROM employees METADATA _source | SORT hire_date | LIMIT 10 | KEEP first_name, _source, hire_date", AUTO),
            FetchExec.class
        );
        assertThat(names(fetch.fetchedAttributes()), containsInAnyOrder("first_name", "_source"));
    }

    /** With {@code AUTO}, a {@code TopN} that defers no {@code _source} saves too little to pay for the round trip. */
    public void testAutoKeepsATopNWithoutTheSourceEager() {
        assertEager(plan(BASIC, AUTO));
        assertThat("the plan can use the fetch phase", plan(BASIC, ALWAYS).collect(FetchExec.class), hasSize(1));
    }

    /** With {@code AUTO}, a {@code LIMIT} stays eager even when it defers {@code _source}: it drops few rows. */
    public void testAutoKeepsALimitEager() {
        String query = "FROM employees METADATA _source | LIMIT 10 | KEEP first_name, _source";
        assertEager(plan(query, AUTO));
        assertThat("the plan can use the fetch phase", plan(query, ALWAYS).collect(FetchExec.class), hasSize(1));
    }

    /**
     * With {@code AUTO}, a relation with one shard stays eager. Its data node cuts the rows down to the global winners,
     * and node-level late materialization loads the deferred columns of those only.
     */
    public void testAutoKeepsASingleShardEager() {
        String query = "FROM employees METADATA _source | SORT hire_date | LIMIT 10 | KEEP first_name, _source";
        assertEager(plan(employeesWithShards(1), query, AUTO, configuration(Settings.EMPTY)));
        assertThat(
            "the same query over three shards gets the fetch phase",
            plan(employeesWithShards(3), query, AUTO, configuration(Settings.EMPTY)).collect(FetchExec.class),
            hasSize(1)
        );
    }

    /**
     * The rule narrows the cut to the documents and the columns that cross as values, and fetches the rest above it.
     * Below the cut, the relation and every projection add {@code _doc}.
     */
    public void testFetchesAboveTheNarrowedCut() {
        Project restore = as(logicalPlan(BASIC, ALWAYS), Project.class);
        Fetch fetch = as(restore.child(), Fetch.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("emp_no", "first_name")));
        Project narrowed = as(fetch.child(), Project.class);
        assertThat(names(narrowed.output()), equalTo(List.of("_doc", "hire_date")));
        assertThat(narrowed.output().getFirst(), equalTo(fetch.doc()));
        TopN cut = as(narrowed.child(), TopN.class);
        EsRelation relation = single(cut, EsRelation.class);
        assertThat(relation.output().getFirst(), equalTo(fetch.doc()));
    }

    /** A plan the rule declines is returned as it came in, so the query plans exactly as without the fetch phase. */
    public void testDeclinedPlansAreUntouched() {
        LogicalPlan eager = logicalPlan("FROM employees | STATS m = MAX(salary) BY hire_date | SORT m | LIMIT 10", DISABLED);
        assertThat(applyTo(eager), sameInstance(eager));
    }

    /** Running the rule on its own output changes nothing: a plan that already fetches is left as it is. */
    public void testRunningTwiceIsANoop() {
        LogicalPlan fetched = logicalPlan(BASIC, ALWAYS);
        assertThat(applyTo(fetched), sameInstance(fetched));
    }

    /** An aggregation before the cut builds each row from several documents, so no row has one {@code _doc}. */
    public void testAggregationBeforeTheCutKeepsThePlanEager() {
        assertEager(plan("FROM employees | STATS m = MAX(salary) BY hire_date | SORT m | LIMIT 10"));
    }

    /** Commands after the cut run on the coordinator over the rows the fetch completed, so they read fetched columns. */
    public void testStatsAfterTheCutReadsTheFetchedColumns() {
        PhysicalPlan plan = plan("FROM employees | SORT hire_date | LIMIT 10 | STATS m = MAX(salary)");
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("salary")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "hire_date")));
    }

    /**
     * The fetch runs after the cut closest to the relation. A second cut after it runs on the coordinator over the fetched
     * rows, so its sort key is fetched too.
     */
    public void testASecondCutRunsAfterTheFetch() {
        PhysicalPlan plan = plan("FROM employees | SORT salary | LIMIT 100 | SORT hire_date | LIMIT 10 | KEEP first_name, hire_date");
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name", "hire_date")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "salary")));
    }

    /**
     * {@code MV_EXPAND} before the cut repeats a row for each value, and every copy keeps the {@code _doc} of its
     * document. The expanded column is computed on the data node, so it crosses the exchange as a value.
     */
    public void testMvExpandBeforeTheCut() {
        PhysicalPlan plan = plan("FROM employees | MV_EXPAND languages | SORT hire_date | LIMIT 10 | KEEP first_name, languages");
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "hire_date", "languages")));
    }

    /**
     * A command before the cut runs on the data nodes. One that only runs on the coordinator splits the plan below the
     * cut, so the plan stays eager.
     */
    public void testCoordinatorCommandBeforeTheCutKeepsThePlanEager() {
        Attribute firstName = field("first_name", DataType.KEYWORD);
        Attribute lastName = field("last_name", DataType.KEYWORD);
        Attribute sort = field("sort", DataType.LONG);
        EsRelation relation = relation(List.of(firstName, lastName, sort), Map.of("", List.of("idx")));
        for (Enrich.Mode mode : Enrich.Mode.values()) {
            Enrich enrich = enrich(relation, mode, firstName);
            LogicalPlan plan = new Project(Source.EMPTY, topN(enrich, sort), List.of(lastName, enrich.output().getLast()));
            LogicalPlan rewritten = applyTo(plan);
            if (mode == Enrich.Mode.COORDINATOR) {
                assertThat(rewritten, sameInstance(plan));
            } else {
                assertThat(mode.toString(), names(single(rewritten, Fetch.class).fetchedAttributes()), equalTo(List.of("last_name")));
            }
        }
    }

    /** A command after the cut runs on the coordinator. One that only runs on the data nodes keeps the plan eager. */
    public void testRemoteCommandAfterTheCutKeepsThePlanEager() {
        Attribute firstName = field("first_name", DataType.KEYWORD);
        Attribute lastName = field("last_name", DataType.KEYWORD);
        Attribute sort = field("sort", DataType.LONG);
        EsRelation relation = relation(List.of(firstName, lastName, sort), Map.of("", List.of("idx")));
        for (Enrich.Mode mode : Enrich.Mode.values()) {
            Enrich enrich = enrich(topN(relation, sort), mode, firstName);
            LogicalPlan plan = new Project(Source.EMPTY, enrich, List.of(lastName, enrich.output().getLast()));
            LogicalPlan rewritten = applyTo(plan);
            if (mode == Enrich.Mode.REMOTE) {
                assertThat(rewritten, sameInstance(plan));
            } else {
                assertThat(mode.toString(), single(rewritten, Fetch.class).fetchedAttributes(), equalTo(List.of(firstName, lastName)));
            }
        }
    }

    public void testNoExchange() {
        assertEager(plan("ROW x = 1"));
        assertWarnings(DEFAULT_LIMIT_WARNING);
    }

    /**
     * The rule names the document column {@code _doc}, so a query that names a column {@code _doc} keeps the plan eager
     * instead of failing on the duplicate name, wherever the query defines it.
     */
    public void testAColumnNamedDocKeepsThePlanEager() {
        assertEager(
            plan("FROM employees METADATA _source | EVAL _doc = emp_no | SORT hire_date | LIMIT 10 | KEEP first_name, _doc, _source")
        );
        assertEager(
            plan("FROM employees METADATA _source | SORT hire_date | LIMIT 10 | EVAL _doc = emp_no | KEEP first_name, _doc, _source")
        );
    }

    public void testNothingToDefer() {
        assertEager(plan("FROM employees | SORT hire_date | LIMIT 10 | KEEP hire_date"));
    }

    /**
     * A field that may be unmapped is read through a rule that runs on the data node and that the fetch side does not
     * run, so it keeps crossing the exchange while the other columns are fetched.
     */
    public void testPotentiallyUnmappedFieldStaysEager() {
        Attribute unmapped = new FieldAttribute(Source.EMPTY, "unmapped_kw", new PotentiallyUnmappedKeywordEsField("unmapped_kw"));
        Attribute mapped = field("first_name", DataType.KEYWORD);
        LogicalPlan rewritten = applyTo(topNPlan(List.of(mapped, unmapped), Map.of("", List.of("idx"))));
        Fetch fetch = single(rewritten, Fetch.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "unmapped_kw", "sort")));
    }

    /** Rows from a remote cluster cannot be fetched yet, the plan stays eager. */
    public void testRemoteClusterStaysEager() {
        LogicalPlan plan = topNPlan(List.of(field("first_name", DataType.KEYWORD)), Map.of("remote", List.of("idx")));
        assertThat(applyTo(plan), sameInstance(plan));
    }

    /** A local TopN only cuts the rows of one node, so it doesn't pick the global winners the fetch loads. */
    public void testLocalTopNIsNotACut() {
        LogicalPlan plan = topNPlan(List.of(field("first_name", DataType.KEYWORD)), Map.of("", List.of("idx")));
        LogicalPlan localCut = plan.transformDown(TopN.class, topN -> topN.withLocal(true));
        assertThat(applyTo(localCut), sameInstance(localCut));
        assertThat("the same plan with a pipeline breaking cut gets the fetch phase", applyTo(plan).collect(Fetch.class), hasSize(1));
    }

    /**
     * A plan the rule declines logs why at debug level, so a query that keeps loading every column eagerly can be
     * explained from the logs.
     */
    @TestLogging(value = "org.elasticsearch.xpack.esql.optimizer.rules.logical.fetch.PlanFetch:DEBUG", reason = "asserts the DEBUG event")
    public void testLogsWhyItDoesNotPlanTheFetchPhase() {
        MockLog.assertThatLogger(
            () -> assertEager(plan("FROM employees | SORT hire_date | LIMIT 10 | KEEP hire_date")),
            PlanFetch.class,
            new MockLog.SeenEventExpectation(
                "declined",
                PlanFetch.class.getCanonicalName(),
                Level.DEBUG,
                "fetch phase not planned: the cut reads every column, or the others cannot be fetched"
            )
        );
    }

    private static void assertEager(PhysicalPlan plan) {
        assertThat(plan.collect(FetchExec.class), empty());
    }

    private static PhysicalPlan plan(String query) {
        return plan(query, ALWAYS);
    }

    private static PhysicalPlan plan(String query, EsqlFlags flags) {
        return plan(query, flags, configuration(Settings.EMPTY), TransportVersion.current());
    }

    private static PhysicalPlan plan(String query, EsqlFlags flags, Configuration configuration, TransportVersion minimumVersion) {
        return plan(employees(minimumVersion), query, flags, configuration);
    }

    private static LogicalPlan logicalPlan(String query, EsqlFlags flags) {
        Configuration configuration = configuration(Settings.EMPTY);
        return new TestPlannerOptimizer(configuration, employees(TransportVersion.current()).buildAnalyzer(), flags).logicalPlan(query);
    }

    private static TestAnalyzer employees(TransportVersion minimumVersion) {
        return EsqlTestUtils.analyzer()
            .addIndex(EsIndexGenerator.esIndex("employees", mapping(), Map.of("employees", IndexMode.STANDARD)))
            .minimumTransportVersion(minimumVersion);
    }

    /** {@code employees} with a known number of shards. The other analyzers report 0, an unknown count. */
    private static TestAnalyzer employeesWithShards(int shards) {
        Map<String, IndexProperties> properties = Map.of("employees", new IndexProperties(IndexMode.STANDARD, shards));
        return EsqlTestUtils.analyzer()
            .addIndex(new EsIndex("employees", mapping(), properties, Map.of(), Map.of()))
            .minimumTransportVersion(TransportVersion.current());
    }

    private static PhysicalPlan plan(TestAnalyzer analyzer, String query) {
        return plan(analyzer, query, ALWAYS, configuration(Settings.EMPTY));
    }

    private static PhysicalPlan plan(TestAnalyzer analyzer, String query, EsqlFlags flags, Configuration configuration) {
        TestPlannerOptimizer optimizer = new TestPlannerOptimizer(configuration, analyzer.buildAnalyzer(), flags);
        return optimizer.distributedPlan(query);
    }

    /**
     * {@code books}, with a {@code title} text field analyzed with {@code standard}.
     */
    private static TestAnalyzer books() {
        Map<String, EsField> mapping = Map.of(
            "book_no",
            new KeywordEsField("book_no", Map.of(), true, 32766, false, false, EsField.TimeSeriesFieldType.NONE),
            "title",
            new TextEsField(
                "title",
                Map.of(),
                false,
                false,
                EsField.TimeSeriesFieldType.NONE,
                "standard",
                TextEsField.DEFAULT_POSITION_INCREMENT_GAP,
                TextEsField.UnknownAnalyzer.NONE,
                null
            ),
            "year",
            new EsField("year", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        return EsqlTestUtils.analyzer()
            .addIndex(EsIndexGenerator.esIndex("books", mapping, Map.of("books", IndexMode.STANDARD)))
            .minimumTransportVersion(TransportVersion.current());
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

    private static Configuration configuration(Settings pragmas) {
        return EsqlTestUtils.configuration(new QueryPragmas(pragmas));
    }

    private static <T extends PhysicalPlan> T single(PhysicalPlan plan, Class<T> type) {
        List<T> found = plan.collect(type);
        assertThat(plan.toString(), found, hasSize(1));
        return found.getFirst();
    }

    private static <T extends LogicalPlan> T single(LogicalPlan plan, Class<T> type) {
        List<T> found = plan.collect(type);
        assertThat(plan.toString(), found, hasSize(1));
        return found.getFirst();
    }

    /** The projection the rule puts between the fetch and the cut, so the fetched columns don't cross the exchange. */
    private static ProjectExec narrowed(FetchExec fetch) {
        return as(fetch.child(), ProjectExec.class);
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

    private static LogicalPlan applyTo(LogicalPlan plan) {
        LogicalOptimizerContext context = new LogicalOptimizerContext(
            configuration(Settings.EMPTY),
            FoldContext.small(),
            TransportVersion.current(),
            ALWAYS
        );
        return new PlanFetch().apply(plan, context);
    }

    /**
     * The optimized plan of {@code FROM idx | SORT sort | LIMIT 10 | KEEP returned...}, built by hand for fields and
     * relations the analyzer cannot produce in a unit test.
     */
    private static LogicalPlan topNPlan(List<Attribute> returned, Map<String, List<String>> concreteIndices) {
        Attribute sort = field("sort", DataType.LONG);
        List<Attribute> relationOutput = new ArrayList<>(returned);
        relationOutput.add(sort);
        return new Project(Source.EMPTY, topN(relation(relationOutput, concreteIndices), sort), returned);
    }

    private static EsRelation relation(List<Attribute> output, Map<String, List<String>> concreteIndices) {
        return new EsRelation(
            Source.EMPTY,
            "idx",
            IndexMode.STANDARD,
            Map.of("", List.of("idx")),
            concreteIndices,
            Map.of("idx", new IndexProperties(IndexMode.STANDARD, 0)),
            output
        );
    }

    /** {@code SORT sort | LIMIT 10}, the cut the data nodes and the coordinator both apply. */
    private static TopN topN(LogicalPlan child, Attribute sort) {
        List<Order> order = List.of(new Order(Source.EMPTY, sort, Order.OrderDirection.ASC, Order.NullsPosition.LAST));
        return new TopN(Source.EMPTY, child, order, EsqlTestUtils.of(10), false);
    }

    /**
     * {@code ENRICH} in the given mode, built by hand because the analyzer rejects a remote {@code ENRICH} after a cut.
     * It adds {@code language_name}, the last column of its output.
     */
    private static Enrich enrich(LogicalPlan child, Enrich.Mode mode, Attribute matchField) {
        return new Enrich(
            Source.EMPTY,
            child,
            mode,
            Literal.keyword(Source.EMPTY, "languages"),
            matchField,
            new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("languages_idx"), "first_name", List.of("language_name")),
            Map.of("", "languages_idx"),
            List.of(field("language_name", DataType.KEYWORD))
        );
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}
