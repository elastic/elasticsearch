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
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTests;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.analysis.rules.ResolveHighlightIndexKey;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstDocId;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhaseOutcomes.Decision;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhasePolicy.Outcome;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.HighlightExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitByExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNByExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.ReductionPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.compute.operator.topn.TopNOperator.InputOrdering.SORTED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
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

    /**
     * A filter after the cut that reads only a column crossing the exchange drops rows before the fetch, so the fetch
     * loads the other columns only for the rows that pass.
     */
    public void testAFilterOnAColumnThatCrossesTheExchangeRunsBeforeTheFetch() {
        PhysicalPlan plan = plan(
            "FROM employees | SORT hire_date | LIMIT 10 | WHERE hire_date > \"1990-01-01\" | KEEP first_name, hire_date"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        FilterExec filter = as(fetch.child(), FilterExec.class);
        assertThat(referencedNames(filter), equalTo(Set.of("hire_date")));
        assertThat(filter.child(), instanceOf(TopNExec.class));
        assertThat(plan.collect(FilterExec.class), hasSize(1));
    }

    /** A filter on a fetched column needs the fetched values, so it runs after the fetch. */
    public void testAFilterOnAFetchedColumnRunsAfterTheFetch() {
        PhysicalPlan plan = plan("FROM employees | SORT hire_date | LIMIT 10 | WHERE salary > 1000 | KEEP first_name, salary").plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(fetch.child(), instanceOf(TopNExec.class));
        assertThat(single(plan, FilterExec.class).child(), sameInstance(fetch));
    }

    /**
     * Each condition of a filter after the cut runs on its own side of the fetch: the one on the sort key before it, the
     * one on a fetched column after it.
     */
    public void testAFilterSplitsItsConditionsAroundTheFetch() {
        PhysicalPlan plan = plan(
            "FROM employees | SORT hire_date | LIMIT 10 | WHERE hire_date > \"1990-01-01\" AND salary > 1000 | KEEP first_name, salary"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        FilterExec beforeTheFetch = as(fetch.child(), FilterExec.class);
        assertThat(referencedNames(beforeTheFetch), equalTo(Set.of("hire_date")));
        assertThat(beforeTheFetch.child(), instanceOf(TopNExec.class));
        List<FilterExec> afterTheFetch = plan.collect(FilterExec.class).stream().filter(filter -> filter.child() == fetch).toList();
        assertThat(afterTheFetch, hasSize(1));
        assertThat(referencedNames(afterTheFetch.getFirst()), equalTo(Set.of("salary")));
    }

    /** A condition that reads a fetched column runs after the fetch, even when it also reads a column that crosses. */
    public void testADisjunctionThatReadsAFetchedColumnRunsAfterTheFetch() {
        PhysicalPlan plan = plan(
            "FROM employees | SORT hire_date | LIMIT 10 | WHERE hire_date > \"1990-01-01\" OR salary > 1000 | KEEP first_name, salary"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(fetch.child(), instanceOf(TopNExec.class));
        FilterExec filter = single(plan, FilterExec.class);
        assertThat(filter.child(), sameInstance(fetch));
        assertThat(referencedNames(filter), equalTo(Set.of("hire_date", "salary")));
    }

    /**
     * {@code HIGHLIGHT} after the cut runs on the coordinator over the fetched text of the winners, so the fields it
     * highlights are fetched instead of crossing the exchange.
     */
    public void testHighlightAfterTheCutFetchesTheFieldsItHighlights() {
        PhysicalPlan plan = plan(books(), "FROM books | SORT year | LIMIT 10 | HIGHLIGHT \"ring\" ON title | KEEP book_no, highlight_title")
            .plan();
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
        PhysicalPlan plan = plan(books(), "FROM books | HIGHLIGHT \"ring\" ON title | SORT year | LIMIT 10 | KEEP book_no, highlight_title")
            .plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("book_no", "title")));
        assertThat(single(plan, HighlightExec.class).child(), sameInstance(fetch));
    }

    /**
     * A sort on a generated highlight column keeps {@code HIGHLIGHT} before the cut, so the plan stays eager.
     */
    public void testSortOnAHighlightKeepsThePlanEager() {
        Planned planned = plan(
            books(),
            "FROM books | HIGHLIGHT \"ring\" ON title | SORT highlight_title | LIMIT 10 | KEEP book_no, highlight_title"
        );
        assertDeclined(planned, Outcome.INELIGIBLE_SHAPE);
        assertThat(planned.decisions().getFirst().reason(), containsString("[Highlight] runs before the cut"));
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
        ).plan();
        assertThat(single(plan, HighlightExec.class).indexKey().name(), equalTo(ResolveHighlightIndexKey.INDEX_KEY_NAME));
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("title", "_index")));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "book_no")));
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
    }

    /**
     * {@code MV_EXPAND} repeats the {@code _doc} of a row for each of its values. The expanded column is computed before the
     * cut and crosses as a value, the other columns of the document are fetched.
     */
    public void testMvExpandBeforeTheCutFetchesTheColumnsOfTheDocuments() {
        PhysicalPlan plan = plan("FROM employees | MV_EXPAND languages | SORT hire_date | LIMIT 10 | KEEP first_name, languages").plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(names(single(plan, ExchangeExec.class).output()), containsInAnyOrder("_doc", "hire_date", "languages"));
    }

    /**
     * A {@code LOOKUP JOIN} keeps the {@code _doc} of each left row. The columns of the lookup index and the key it looks up
     * cross as values, the other columns of the left rows are fetched.
     */
    public void testALookupJoinBeforeTheCutFetchesTheColumnsOfTheLeftRows() {
        PhysicalPlan plan = planWithLanguagesLookup(
            "FROM employees | LOOKUP JOIN languages_lookup ON language_code | SORT hire_date | LIMIT 10"
                + " | KEEP first_name, language_code, language_name"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(
            names(single(plan, ExchangeExec.class).output()),
            containsInAnyOrder("_doc", "hire_date", "language_code", "language_name")
        );
    }

    /** Only the relation the rows come from gets {@code _doc}. The lookup index of a join has no {@code _doc} to give. */
    public void testTheLookupIndexOfAJoinGetsNoDoc() {
        PhysicalPlan plan = planWithLanguagesLookup(
            "FROM employees | LOOKUP JOIN languages_lookup ON language_code | SORT hire_date | LIMIT 10 | KEEP first_name, language_name"
        ).plan();
        FragmentExec fragment = single(plan, FragmentExec.class);
        List<EsRelation> relations = fragment.fragment().collect(EsRelation.class);
        assertThat(relations, hasSize(2));
        for (EsRelation relation : relations) {
            boolean carriesDoc = relation.output().stream().anyMatch(EsQueryExec::isDocAttribute);
            assertThat(relation.indexPattern(), carriesDoc, equalTo(relation.indexMode() != IndexMode.LOOKUP));
        }
    }

    /**
     * {@code CHANGE_POINT} keeps each row tied to its document, but it runs only on the coordinator, after the cut. Its
     * exec doesn't say that a detached {@code _doc} can pass through it, so the plan stays eager.
     */
    public void testACommandThatKeepsTheDocumentsButRunsOnTheCoordinatorKeepsThePlanEager() {
        assertIneligible(
            "FROM employees | CHANGE_POINT salary ON hire_date | KEEP first_name, hire_date, salary, type, pvalue",
            "[ChangePointExec] runs after the cut"
        );
        assertWarnings(DEFAULT_LIMIT_WARNING);
    }

    /** {@code _doc} passes through a projection on the data nodes, so every projection of the fragment keeps it. */
    public void testProjectionBeforeTheCutKeepsTheDocuments() {
        PhysicalPlan plan = applyTo(topNPlanWithBeforeTheCut(relation -> new Project(Source.EMPTY, relation, relation.output())));
        FetchExec fetch = single(plan, FetchExec.class);
        Project fragmentRoot = as(single(plan, FragmentExec.class).fragment(), Project.class);
        Project beforeTheCut = as(as(fragmentRoot.child(), TopN.class).child(), Project.class);
        assertThat(beforeTheCut.output().getFirst(), equalTo(fetch.doc()));
    }

    /** A local {@code LIMIT} only cuts the rows of one node, and {@code _doc} passes through it on the data nodes. */
    public void testLocalLimitBeforeTheCutKeepsTheFetch() {
        PhysicalPlan plan = topNPlanWithBeforeTheCut(relation -> new Limit(Source.EMPTY, EsqlTestUtils.of(100), relation, false, true));
        FetchExec fetch = single(applyTo(plan), FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
    }

    /** A local {@code TopN} only cuts the rows of one node, and {@code _doc} passes through it on the data nodes. */
    public void testLocalTopNBeforeTheCutKeepsTheFetch() {
        PhysicalPlan plan = topNPlanWithBeforeTheCut(
            relation -> new TopN(Source.EMPTY, relation, sortOn(relation.output().getLast()), EsqlTestUtils.of(100), true)
        );
        FetchExec fetch = single(applyTo(plan), FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
    }

    /**
     * Each branch sends the documents of its candidates with the columns its cut and {@code FUSE} read. {@code FUSE} keeps
     * the {@code _doc} of each group, and the fetch loads the other columns for the rows of the final cut.
     */
    public void testFuseFetchesTheColumnsOfTheFinalWinners() {
        PhysicalPlan plan = plan(
            "FROM employees METADATA _id, _index, _score"
                + " | FORK (WHERE salary > 1000 | SORT hire_date DESC | LIMIT 50) (WHERE languages > 2 | SORT hire_date DESC | LIMIT 50)"
                + " | FUSE | SORT _score DESC | LIMIT 10 | KEEP first_name, emp_no, _score"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), containsInAnyOrder("emp_no", "first_name"));
        TopNExec finalCut = as(fetch.child(), TopNExec.class);
        AggregateExec fuse = as(finalCut.child(), AggregateExec.class);
        Alias firstDoc = as(fuse.aggregates().getLast(), Alias.class);
        assertThat(firstDoc.child(), instanceOf(FirstDocId.class));
        assertThat(fetch.doc().id(), equalTo(firstDoc.id()));
        List<ExchangeExec> exchanges = plan.collect(ExchangeExec.class);
        assertThat(exchanges, hasSize(2));
        for (ExchangeExec exchange : exchanges) {
            assertThat(names(exchange.output()), equalTo(List.of("_doc", "hire_date", "_id", "_index", "_score")));
        }
    }

    /** {@code _version} is the same metadata in every branch, so {@code FUSE} lets the fetch load it for the final winners. */
    public void testFuseFetchesTheMetadataOfTheFinalWinners() {
        PhysicalPlan plan = plan(
            "FROM employees METADATA _id, _index, _score, _version"
                + " | FORK (WHERE salary > 1000 | SORT hire_date DESC | LIMIT 50) (WHERE languages > 2 | SORT hire_date DESC | LIMIT 50)"
                + " | FUSE | SORT _score DESC | LIMIT 10 | KEEP _version, _score"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("_version")));
        assertThat(fetch.fetchedAttributes().getFirst(), instanceOf(MetadataAttribute.class));
        for (ExchangeExec exchange : plan.collect(ExchangeExec.class)) {
            assertThat(names(exchange.output()), not(hasItem("_version")));
        }
    }

    /**
     * Without {@code FUSE}, the rows of every branch reach the final cut as they are, and the fetch reads the {@code _doc}
     * of the merge.
     */
    public void testForkFetchesTheColumnsOfTheFinalCut() {
        PhysicalPlan plan = plan(
            "FROM employees"
                + " | FORK (WHERE salary > 1000 | SORT hire_date DESC | LIMIT 50) (WHERE languages > 2 | SORT hire_date DESC | LIMIT 50)"
                + " | SORT hire_date DESC | LIMIT 10 | KEEP first_name, emp_no, hire_date"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), containsInAnyOrder("emp_no", "first_name"));
        MergeExec merge = as(as(fetch.child(), TopNExec.class).child(), MergeExec.class);
        assertThat(merge.output().getFirst(), equalTo(fetch.doc()));
        for (PhysicalPlan branch : merge.children()) {
            assertTrue(EsQueryExec.isDocAttribute(branch.output().getFirst()));
            assertThat(names(branch.output()), not(hasItem("first_name")));
        }
    }

    /** {@code _source} is the same metadata in every branch, so the fetch loads it for the rows of the final cut. */
    public void testForkFetchesTheSourceOfTheFinalCut() {
        PhysicalPlan plan = plan(
            "FROM employees METADATA _source"
                + " | FORK (WHERE salary > 1000 | SORT hire_date DESC | LIMIT 50) (WHERE languages > 2 | SORT hire_date DESC | LIMIT 50)"
                + " | SORT hire_date DESC | LIMIT 10 | KEEP _source, hire_date"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("_source")));
        assertThat(fetch.fetchedAttributes().getFirst(), instanceOf(MetadataAttribute.class));
        for (ExchangeExec exchange : plan.collect(ExchangeExec.class)) {
            assertThat(names(exchange.output()), not(hasItem("_source")));
        }
    }

    /** The final cut sorts on a column no branch cut reads, so every branch keeps sending it. */
    public void testAColumnTheFinalCutReadsStaysEager() {
        PhysicalPlan plan = plan(
            "FROM employees"
                + " | FORK (WHERE salary > 1000 | SORT hire_date DESC | LIMIT 50) (WHERE languages > 2 | SORT hire_date DESC | LIMIT 50)"
                + " | SORT emp_no | LIMIT 10 | KEEP first_name, emp_no, hire_date"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        for (ExchangeExec exchange : plan.collect(ExchangeExec.class)) {
            assertThat(names(exchange.output()), hasItem("emp_no"));
        }
    }

    /** A column a branch reads after its own cut must be there as a value, so every branch keeps sending it. */
    public void testAColumnABranchReadsAfterItsCutStaysEager() {
        PhysicalPlan plan = plan(
            "FROM employees"
                + " | FORK (SORT hire_date DESC | LIMIT 50 | WHERE first_name != \"x\") (SORT hire_date DESC | LIMIT 50)"
                + " | SORT hire_date DESC | LIMIT 10 | KEEP first_name, emp_no, hire_date"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("emp_no")));
        for (ExchangeExec exchange : plan.collect(ExchangeExec.class)) {
            assertThat(names(exchange.output()), hasItem("first_name"));
        }
    }

    /** The final cut sorts on the {@code FIRST} that {@code FUSE} keeps of a column, so every branch keeps sending it. */
    public void testAColumnTheFinalCutReadsAfterFuseStaysEager() {
        PhysicalPlan plan = plan(
            "FROM employees METADATA _id, _index, _score"
                + " | FORK (WHERE salary > 1000 | SORT hire_date DESC | LIMIT 50) (WHERE languages > 2 | SORT hire_date DESC | LIMIT 50)"
                + " | FUSE | SORT first_name, _score DESC | LIMIT 10 | KEEP first_name, emp_no, _score"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("emp_no")));
        for (ExchangeExec exchange : plan.collect(ExchangeExec.class)) {
            assertThat(names(exchange.output()), hasItem("first_name"));
        }
    }

    /**
     * {@code FUSE} by another key can put several documents in a group, so its {@code FIRST} of a column isn't the value of
     * the document it would keep, and the plan stays eager.
     */
    public void testFuseByAnotherKeyKeepsThePlanEager() {
        assertIneligible(
            "FROM employees METADATA _id, _index, _score"
                + " | FORK (WHERE salary > 1000 | SORT hire_date DESC | LIMIT 50) (WHERE languages > 2 | SORT hire_date DESC | LIMIT 50)"
                + " | FUSE KEY BY first_name | SORT _score DESC | LIMIT 10 | KEEP first_name, emp_no, _score",
            "[AggregateExec] runs between the cut and the [FORK]"
        );
    }

    /** A filter right after the final cut on a column that crosses the exchanges drops rows before the fetch. */
    public void testAFilterAfterTheFinalCutRunsBeforeTheFetch() {
        PhysicalPlan plan = plan(
            "FROM employees"
                + " | FORK (WHERE salary > 1000 | SORT hire_date DESC | LIMIT 50) (WHERE languages > 2 | SORT hire_date DESC | LIMIT 50)"
                + " | SORT hire_date DESC | LIMIT 10 | WHERE hire_date > \"1990-01-01\" | KEEP first_name, emp_no, hire_date"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        FilterExec filter = as(fetch.child(), FilterExec.class);
        assertThat(referencedNames(filter), equalTo(Set.of("hire_date")));
        assertThat(filter.child(), instanceOf(TopNExec.class));
    }

    /** After the final cut of a {@code FORK}, only execs a detached {@code _doc} can pass through may run. */
    public void testAStatsAfterTheFinalCutKeepsTheForkEager() {
        assertIneligible(
            "FROM employees"
                + " | FORK (WHERE salary > 1000 | SORT hire_date DESC | LIMIT 50) (WHERE languages > 2 | SORT hire_date DESC | LIMIT 50)"
                + " | SORT hire_date DESC | LIMIT 10 | STATS m = MAX(salary)",
            "[AggregateExec] runs after the cut"
        );
    }

    /** Running the rule on the {@code FORK} it planned changes nothing: the fetch now runs after the final cut. */
    public void testRunningTwiceOnAForkIsANoop() {
        PhysicalPlan fetched = plan(
            "FROM employees METADATA _id, _index, _score"
                + " | FORK (WHERE salary > 1000 | SORT hire_date DESC | LIMIT 50) (WHERE languages > 2 | SORT hire_date DESC | LIMIT 50)"
                + " | FUSE | SORT _score DESC | LIMIT 10 | KEEP first_name, emp_no, _score"
        ).plan();
        FetchPhaseOutcomes outcomes = new FetchPhaseOutcomes();
        assertThat(new PlanFetch(outcomes).apply(fetched, context(EsqlFlags.withFetchPhase(true))), sameInstance(fetched));
        assertThat(outcomes.all(), contains(new Decision(Outcome.INELIGIBLE_SHAPE, "[FetchExec] runs after the cut")));
    }

    /**
     * A second cut after the coordinator cut is the final cut: the fetch loads its 10 rows, and the column it sorts on
     * crosses the exchange.
     */
    public void testChainedCutsFetchTheRowsOfTheLastCut() {
        PhysicalPlan plan = plan("FROM employees | SORT salary | LIMIT 100 | SORT hire_date | LIMIT 10 | KEEP first_name, hire_date")
            .plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        TopNExec finalCut = as(fetch.child(), TopNExec.class);
        assertThat(names(finalCut.order().stream().map(order -> (Attribute) order.child()).toList()), equalTo(List.of("hire_date")));
        assertThat(as(finalCut.child(), TopNExec.class).child(), instanceOf(ExchangeExec.class));
        assertThat(names(single(plan, ExchangeExec.class).output()), containsInAnyOrder("_doc", "hire_date", "salary"));
    }

    /** What runs between the cuts reads its columns before the fetch, so they cross the exchange. */
    public void testAnEvalBetweenTheCutsReadsItsInputBeforeTheFetch() {
        PhysicalPlan plan = plan("FROM employees | SORT salary | LIMIT 100 | EVAL x = salary * 2 | SORT x | LIMIT 10 | KEEP first_name, x")
            .plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(as(fetch.child(), TopNExec.class).child(), instanceOf(EvalExec.class));
        assertThat(names(single(plan, ExchangeExec.class).output()), containsInAnyOrder("_doc", "salary"));
    }

    /**
     * The implicit {@code LIMIT} of the query sits above a {@code WHERE} that follows the cut, and keeps every one of its
     * 10 rows. It is not the final cut, so the {@code WHERE} can read the fetched {@code salary}.
     */
    public void testTheImplicitLimitAfterAWhereIsNotTheFinalCut() {
        PhysicalPlan plan = plan("FROM employees | SORT hire_date | LIMIT 10 | WHERE salary > 50000 | KEEP first_name").plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), containsInAnyOrder("first_name", "salary"));
        assertThat(fetch.child(), instanceOf(TopNExec.class));
        assertThat(single(plan, LimitExec.class).child(), instanceOf(FilterExec.class));
    }

    /**
     * A projection between the cuts passes {@code _doc} on and leaves out the columns the fetch loads above it. Built by
     * hand because the optimizer moves the projections of a query above its cuts.
     */
    public void testAProjectionBetweenTheCutsPassesTheDocOn() {
        Attribute fetched = field("first_name", DataType.KEYWORD);
        Attribute secondKey = field("other", DataType.LONG);
        ProjectExec firstCutAndProjection = (ProjectExec) topNPlan(List.of(fetched, secondKey), Map.of("", List.of("idx")));
        TopNExec secondCut = new TopNExec(Source.EMPTY, firstCutAndProjection, sortOn(secondKey), EsqlTestUtils.of(5), null);
        PhysicalPlan plan = new ProjectExec(Source.EMPTY, secondCut, List.of(fetched));

        FetchExec fetch = single(applyTo(plan), FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        ProjectExec between = as(as(fetch.child(), TopNExec.class).child(), ProjectExec.class);
        assertThat(names(between.output()), equalTo(List.of("_doc", "other")));
        assertThat(between.output().getFirst(), equalTo(fetch.doc()));
    }

    /**
     * {@code SORT ... | LIMIT n BY} repeats on the coordinator like a {@code TopN}. The implicit {@code LIMIT} of the query
     * above it is the final cut, and the groups and the sort key cross the exchange.
     */
    public void testATopNByCutFetchesTheRowsOfTheLimitAboveIt() {
        PhysicalPlan plan = plan("FROM employees | SORT hire_date | LIMIT 2 BY languages | KEEP first_name, languages, hire_date").plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(as(fetch.child(), LimitExec.class).child(), instanceOf(TopNByExec.class));
        assertThat(names(single(plan, ExchangeExec.class).output()), containsInAnyOrder("_doc", "hire_date", "languages"));
        assertWarnings(DEFAULT_LIMIT_WARNING);
    }

    /** {@code LIMIT n BY} repeats on the coordinator like a {@code LIMIT}, and its groups cross the exchange. */
    public void testALimitByCutFetchesTheRowsOfTheLimitAboveIt() {
        PhysicalPlan plan = plan("FROM employees | LIMIT 2 BY languages | KEEP first_name, languages").plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(as(fetch.child(), LimitExec.class).child(), instanceOf(LimitByExec.class));
        assertThat(names(single(plan, ExchangeExec.class).output()), containsInAnyOrder("_doc", "languages"));
        assertWarnings(DEFAULT_LIMIT_WARNING);
    }

    /** A {@code LIMIT BY} after a cut of 100 rows can drop more of them, so it is the final cut. */
    public void testALimitByAfterTheCutIsTheFinalCut() {
        PhysicalPlan plan = plan("FROM employees | SORT salary | LIMIT 100 | LIMIT 2 BY languages | KEEP first_name, languages").plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(as(fetch.child(), LimitByExec.class).child(), instanceOf(TopNExec.class));
    }

    /** A {@code SORT ... | LIMIT n BY} after a cut of 100 rows can drop more of them, so it is the final cut. */
    public void testATopNByAfterTheCutIsTheFinalCut() {
        PhysicalPlan plan = plan(
            "FROM employees | SORT salary | LIMIT 100 | SORT hire_date | LIMIT 2 BY languages | KEEP first_name, languages, hire_date"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        assertThat(as(fetch.child(), TopNByExec.class).child(), instanceOf(TopNExec.class));
    }

    /** After a {@code FORK} the fetch also runs after the last cut that can drop rows. */
    public void testAForkFetchesTheRowsOfItsLastCut() {
        PhysicalPlan plan = plan(
            "FROM employees"
                + " | FORK (WHERE salary > 1000 | SORT hire_date DESC | LIMIT 50) (WHERE languages > 2 | SORT hire_date DESC | LIMIT 50)"
                + " | SORT hire_date DESC | LIMIT 20 | SORT emp_no | LIMIT 10 | KEEP first_name, emp_no, hire_date"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), equalTo(List.of("first_name")));
        TopNExec finalCut = as(fetch.child(), TopNExec.class);
        assertThat(as(finalCut.child(), TopNExec.class).child(), instanceOf(MergeExec.class));
    }

    public void testNoExchange() {
        assertIneligible("ROW x = 1", "no exchange");
        assertWarnings(DEFAULT_LIMIT_WARNING);
    }

    public void testNothingToDefer() {
        assertDeclined(plan("FROM employees | SORT hire_date | LIMIT 10 | KEEP hire_date"), Outcome.INELIGIBLE_NO_DEFERRABLE_FIELDS);
    }

    /** A field that may be unmapped is fetched like a mapped one, and the fetch side reads it as the data node would. */
    public void testAPotentiallyUnmappedFieldIsFetched() {
        PhysicalPlan plan = planWithUnmappedFields(
            UnmappedResolution.LOAD,
            "FROM employees | SORT hire_date | LIMIT 10 | KEEP first_name, does_not_exist"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), containsInAnyOrder("first_name", "does_not_exist"));
        FieldAttribute unmapped = (FieldAttribute) fetch.fetchedAttributes()
            .stream()
            .filter(attribute -> attribute.name().equals("does_not_exist"))
            .findFirst()
            .orElseThrow();
        assertThat(unmapped.field(), instanceOf(PotentiallyUnmappedKeywordEsField.class));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "hire_date")));
    }

    /** The unmapped fields of {@code unmapped_fields="load_all"} are one {@code _source} shaped column, fetched like {@code _source}. */
    public void testTheUnmappedFieldsOfLoadAllAreFetched() {
        PhysicalPlan plan = planWithUnmappedFields(
            UnmappedResolution.LOAD_ALL,
            "FROM employees | SORT hire_date | LIMIT 10 | KEEP first_name*, hire_date"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), containsInAnyOrder("first_name", UnmappedFieldsAttribute.ATTRIBUTE_NAME));
        assertThat(names(fetch.child().output()), equalTo(List.of("_doc", "hire_date")));
    }

    /** Every branch of a {@code FORK} reads the unmapped fields of one pattern, so the fetch loads them for the final cut. */
    public void testAForkFetchesTheUnmappedFieldsOfLoadAll() {
        PhysicalPlan plan = planWithUnmappedFields(
            UnmappedResolution.LOAD_ALL,
            "FROM employees"
                + " | FORK (WHERE salary > 1000 | SORT hire_date DESC | LIMIT 50) (WHERE languages > 2 | SORT hire_date DESC | LIMIT 50)"
                + " | SORT hire_date DESC | LIMIT 10 | KEEP first_name*, hire_date"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), hasItem(UnmappedFieldsAttribute.ATTRIBUTE_NAME));
        for (ExchangeExec exchange : plan.collect(ExchangeExec.class)) {
            assertThat(names(exchange.output()), not(hasItem(UnmappedFieldsAttribute.ATTRIBUTE_NAME)));
        }
    }

    /**
     * A branch that drops some unmapped fields reads them with another pattern than the other branch, so one fetch can't
     * load the column for the rows of both, and every branch keeps sending it.
     */
    public void testAForkWhoseBranchesReadOtherUnmappedFieldsKeepsThemEager() {
        PhysicalPlan plan = planWithUnmappedFields(
            UnmappedResolution.LOAD_ALL,
            "FROM employees"
                + " | FORK (WHERE salary > 1000 | SORT hire_date DESC | LIMIT 50 | DROP does_not_exist*)"
                + " (WHERE languages > 2 | SORT hire_date DESC | LIMIT 50)"
                + " | SORT hire_date DESC | LIMIT 10 | KEEP first_name*, hire_date"
        ).plan();
        FetchExec fetch = single(plan, FetchExec.class);
        assertThat(names(fetch.fetchedAttributes()), not(hasItem(UnmappedFieldsAttribute.ATTRIBUTE_NAME)));
        for (ExchangeExec exchange : plan.collect(ExchangeExec.class)) {
            assertThat(names(exchange.output()), hasItem(UnmappedFieldsAttribute.ATTRIBUTE_NAME));
        }
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
        TestAnalyzer employees = EsqlTestUtils.analyzer()
            .addIndex(EsIndexGenerator.esIndex("employees", mapping(), Map.of("employees", IndexMode.STANDARD)))
            .minimumTransportVersion(minimumVersion);
        return plan(employees, query, flags, configuration);
    }

    /** {@code query} over {@code employees} with {@code SET unmapped_fields} at {@code unmappedFields}. */
    private static Planned planWithUnmappedFields(UnmappedResolution unmappedFields, String query) {
        TestAnalyzer employees = EsqlTestUtils.analyzer()
            .addIndex(EsIndexGenerator.esIndex("employees", mapping(), Map.of("employees", IndexMode.STANDARD)))
            .minimumTransportVersion(TransportVersion.current())
            .unmappedResolution(unmappedFields);
        return plan(employees, query);
    }

    /** {@code query} over {@code employees} with a {@code language_code} field, next to the {@code languages_lookup} index. */
    private static Planned planWithLanguagesLookup(String query) {
        Map<String, EsField> mapping = new HashMap<>(mapping());
        mapping.put("language_code", new EsField("language_code", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        TestAnalyzer employees = EsqlTestUtils.analyzer()
            .addIndex(EsIndexGenerator.esIndex("employees", mapping, Map.of("employees", IndexMode.STANDARD)))
            .addLanguagesLookup()
            .minimumTransportVersion(TransportVersion.current());
        return plan(employees, query);
    }

    private static Planned plan(TestAnalyzer analyzer, String query) {
        return plan(analyzer, query, EsqlFlags.withFetchPhase(true), configuration(Settings.EMPTY));
    }

    private static Planned plan(TestAnalyzer analyzer, String query, EsqlFlags flags, Configuration configuration) {
        TestPlannerOptimizer optimizer = new TestPlannerOptimizer(configuration, analyzer.buildAnalyzer(), flags);
        PhysicalPlan plan = optimizer.distributedPlan(query);
        return new Planned(plan, optimizer.fetchPhaseOutcomes().all());
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

    private static Set<String> referencedNames(FilterExec filter) {
        return filter.condition().references().stream().map(Attribute::name).collect(Collectors.toSet());
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
        List<Order> order = sortOn(sort);
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

    /**
     * {@link #topNPlan} returning {@code first_name}, with {@code beforeTheCut} between the cut that ends the fragment and
     * the relation.
     */
    private static PhysicalPlan topNPlanWithBeforeTheCut(Function<EsRelation, LogicalPlan> beforeTheCut) {
        PhysicalPlan plan = topNPlan(List.of(field("first_name", DataType.KEYWORD)), Map.of("", List.of("idx")));
        return plan.transformDown(
            FragmentExec.class,
            fragment -> fragment.withFragment(fragment.fragment().transformUp(EsRelation.class, beforeTheCut))
        );
    }

    private static List<Order> sortOn(Attribute key) {
        return List.of(new Order(Source.EMPTY, key, Order.OrderDirection.ASC, Order.NullsPosition.LAST));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}
