/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.IpLocation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegisteredDomain;
import org.elasticsearch.xpack.esql.plan.logical.UriParts;
import org.elasticsearch.xpack.esql.plan.logical.UserAgent;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.DenseVector;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.rule.RuleExecutionException;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class HoistProjectsTests extends AbstractLogicalPlanOptimizerTests {

    public HoistProjectsTests(VersionMode versionMode) {
        super(versionMode);
    }

    public void testJoinOverRenamedKeyIsHoisted() {
        Join join = hoistedPast("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            """, Join.class);

        assertThat(Expressions.names(join.config().leftFields()), contains("languages"));
        as(join.left(), EsRelation.class);
    }

    public void testJoinOverKeepIsHoisted() {
        Join join = hoistedPast("""
            FROM test
            | EVAL language_code = languages + 1
            | KEEP emp_no, language_code
            | LOOKUP JOIN languages_lookup ON language_code
            """, Join.class);

        assertThat(Expressions.names(join.config().leftFields()), contains("language_code"));
        as(join.left(), Eval.class);
    }

    public void testJoinShadowingARenamedColumnIsHoistedWithATemporaryName() {
        Join join = hoistedPast("""
            FROM test
            | RENAME first_name AS original_first_name
            | LOOKUP JOIN test_lookup ON emp_no
            """, Join.class);

        Eval temporaryNames = as(join.left(), Eval.class);
        assertThat(Expressions.names(temporaryNames.fields()), contains(startsWith("$$first_name")));
    }

    public void testProjectIsNotHoistedPastInlineJoin() {
        LogicalPlan plan = new HoistProjects().apply(substituted("""
            FROM test
            | RENAME languages AS language_code
            | INLINE STATS max_salary = MAX(salary) BY language_code
            """));

        var inlineJoin = plan.collect(InlineJoin.class).getFirst();
        as(inlineJoin.left(), Project.class);
    }

    public void testProjectOverProjectIsCombined() {
        LogicalPlan substituted = substituted("""
            FROM test
            | RENAME first_name AS a
            | EVAL x = 1
            | RENAME a AS b
            | DROP x
            """);
        assertTrue(hasProjectChild(substituted, Project.class));

        LogicalPlan plan = new HoistProjects().apply(substituted);

        var project = as(plan, Project.class);
        assertThat(plan.collect(Project.class), hasSize(1));
        assertThat(plan.output(), equalTo(substituted.output()));
        Alias b = (Alias) project.projections().stream().filter(p -> p.name().equals("b")).findFirst().orElseThrow();
        assertThat(Expressions.name(b.child()), equalTo("first_name"));
    }

    public void testEvalOverRenameIsPushedBelowTheProject() {
        Eval eval = hoistedPast("""
            FROM test
            | RENAME salary AS s
            | EVAL double_s = s * 2
            """, Eval.class);

        assertThat(eval.references().names(), equalTo(Set.of("salary")));
        as(eval.child(), EsRelation.class);
    }

    public void testEvalShadowingARenamedColumnGetsATemporaryName() {
        Eval eval = hoistedPast("""
            FROM test
            | RENAME salary AS s
            | EVAL salary = s + 1
            """, Eval.class);

        assertThat(Expressions.names(eval.fields()), contains(startsWith("$$salary")));
    }

    public void testLimitOverKeepIsSwapped() {
        LogicalPlan plan = hoistedOnce("""
            FROM test
            | KEEP emp_no, salary
            | LIMIT 5
            """, Limit.class);

        Limit limit = plan.collect(Limit.class)
            .stream()
            .filter(l -> as(l.limit(), Literal.class).value().equals(5))
            .findFirst()
            .orElseThrow();
        as(limit.child(), EsRelation.class);
    }

    public void testLimitKeepsItsDuplicatedAndLocalFlags() {
        EsRelation relation = relation();
        Project project = new Project(EMPTY, relation, relation.output());

        LogicalPlan plan = hoistedOnce(new Limit(EMPTY, new Literal(EMPTY, 10, DataType.INTEGER), project, true, true), Limit.class);

        Limit limit = as(as(plan, Project.class).child(), Limit.class);
        assertTrue(limit.duplicated());
        assertTrue(limit.local());
    }

    public void testFilterOverRenameIsPushedBelowTheProject() {
        Filter filter = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | WHERE fn == "Georgi"
            """, Filter.class);

        assertThat(filter.condition().references().names(), equalTo(Set.of("first_name")));
        as(filter.child(), EsRelation.class);
    }

    public void testFilterOverKeepIsPushedBelowTheProject() {
        Filter filter = hoistedPast("""
            FROM test
            | KEEP emp_no, salary
            | WHERE salary > 40000
            """, Filter.class);

        assertThat(filter.condition().references().names(), equalTo(Set.of("salary")));
        as(filter.child(), EsRelation.class);
    }

    public void testDissectOverRenameIsPushedBelowTheProject() {
        Dissect dissect = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | DISSECT fn "%{head} %{tail}"
            """, Dissect.class);

        assertThat(dissect.input().references().names(), equalTo(Set.of("first_name")));
        assertThat(Expressions.names(dissect.extractedFields()), contains("head", "tail"));
    }

    public void testDissectShadowingARenamedColumnGetsATemporaryName() {
        Dissect dissect = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | DISSECT fn "%{first_name}"
            """, Dissect.class);

        assertThat(Expressions.names(dissect.extractedFields()), contains(startsWith("$$first_name")));
    }

    public void testGrokOverRenameIsPushedBelowTheProject() {
        Grok grok = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | GROK fn "%{WORD:word}"
            """, Grok.class);

        assertThat(grok.input().references().names(), equalTo(Set.of("first_name")));
        assertThat(Expressions.names(grok.extractedFields()), contains("word"));
    }

    public void testGrokShadowingARenamedColumnGetsATemporaryName() {
        Grok grok = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | GROK fn "%{WORD:first_name}"
            """, Grok.class);

        assertThat(Expressions.names(grok.extractedFields()), contains(startsWith("$$first_name")));
    }

    public void testEnrichOverRenameIsPushedBelowTheProject() {
        Enrich enrich = hoistedPast("""
            FROM test
            | RENAME languages AS lc
            | ENRICH languages_idx ON lc WITH language_name
            """, Enrich.class);

        assertThat(enrich.matchField().name(), equalTo("languages"));
        as(enrich.child(), EsRelation.class);
    }

    public void testEnrichShadowingARenamedColumnGetsATemporaryName() {
        Enrich enrich = hoistedPast("""
            FROM test
            | RENAME languages AS lc
            | ENRICH languages_idx ON lc WITH languages = language_name
            """, Enrich.class);

        assertThat(Expressions.names(enrich.generatedAttributes()), contains(startsWith("$$languages")));
    }

    public void testUriPartsOverRenameIsPushedBelowTheProject() {
        assumeTrue("requires uri_parts command capability", EsqlCapabilities.Cap.URI_PARTS_COMMAND.isEnabled());
        UriParts uriParts = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | URI_PARTS parts = fn
            """, UriParts.class);

        assertThat(uriParts.references().names(), equalTo(Set.of("first_name")));
    }

    public void testRegisteredDomainOverRenameIsPushedBelowTheProject() {
        assumeTrue("requires registered_domain command capability", EsqlCapabilities.Cap.REGISTERED_DOMAIN_COMMAND.isEnabled());
        RegisteredDomain registeredDomain = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | REGISTERED_DOMAIN rd = fn
            """, RegisteredDomain.class);

        assertThat(registeredDomain.references().names(), equalTo(Set.of("first_name")));
    }

    public void testUserAgentOverRenameIsPushedBelowTheProject() {
        assumeTrue("requires user_agent command capability", EsqlCapabilities.Cap.USER_AGENT_COMMAND.isEnabled());
        UserAgent userAgent = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | USER_AGENT ua = fn
            """, UserAgent.class);

        assertThat(userAgent.references().names(), equalTo(Set.of("first_name")));
    }

    public void testIpLocationOverRenameIsPushedBelowTheProject() {
        assumeTrue("requires ip_location command capability", EsqlCapabilities.Cap.IP_LOCATION_COMMAND.isEnabled());
        IpLocation ipLocation = hoistedPast("""
            FROM test
            | RENAME first_name AS fn
            | IP_LOCATION g = fn
            """, IpLocation.class);

        assertThat(ipLocation.references().names(), equalTo(Set.of("first_name")));
    }

    public void testCompletionOverRenameIsPushedBelowTheProject() {
        EsRelation relation = relation();
        Attribute field = relation.output().getFirst();
        Alias renamed = new Alias(EMPTY, "renamed", field);
        Project project = new Project(EMPTY, relation, List.of(renamed));

        LogicalPlan plan = hoistedOnce(completion(project, renamed.toAttribute(), "completion"), Completion.class);

        Completion completion = as(as(plan, Project.class).child(), Completion.class);
        assertThat(completion.prompt(), equalTo(field));
        as(completion.child(), EsRelation.class);
    }

    public void testCompletionShadowingARenamedColumnGetsATemporaryName() {
        EsRelation relation = relation();
        Attribute field = relation.output().getFirst();
        Alias renamed = new Alias(EMPTY, "renamed", field);
        Project project = new Project(EMPTY, relation, List.of(renamed));

        LogicalPlan plan = hoistedOnce(completion(project, renamed.toAttribute(), field.name()), Completion.class);

        Completion completion = as(as(plan, Project.class).child(), Completion.class);
        assertThat(completion.targetField().name(), startsWith("$$" + field.name()));
    }

    public void testRerankOverRenameIsPushedBelowTheProject() {
        EsRelation relation = relation();
        Attribute field = relation.output().getFirst();
        Alias renamed = new Alias(EMPTY, "renamed", field);
        Project project = new Project(EMPTY, relation, List.of(renamed));
        Rerank rerankOverProject = new Rerank(
            EMPTY,
            project,
            new Literal(EMPTY, 1000, DataType.INTEGER),
            Literal.keyword(EMPTY, "query"),
            List.of(new Alias(EMPTY, "renamed", renamed.toAttribute())),
            referenceAttribute("_score", DataType.DOUBLE)
        );

        LogicalPlan plan = hoistedOnce(rerankOverProject, Rerank.class);

        Rerank rerank = as(as(plan, Project.class).child(), Rerank.class);
        assertThat(rerank.rerankFields().getFirst().child(), equalTo(field));
        as(rerank.child(), EsRelation.class);
    }

    public void testDenseVectorOverRenameIsPushedBelowTheProject() {
        EsRelation relation = relation();
        Attribute field = relation.output().getFirst();
        Alias renamed = new Alias(EMPTY, "renamed", field);
        Project project = new Project(EMPTY, relation, List.of(renamed));
        List<NamedExpression> fields = List.of(renamed.toAttribute());
        DenseVector denseVectorOverProject = new DenseVector(EMPTY, project, new Literal(EMPTY, 1000, DataType.INTEGER), fields);

        LogicalPlan plan = hoistedOnce(denseVectorOverProject, DenseVector.class);

        DenseVector denseVector = as(as(plan, Project.class).child(), DenseVector.class);
        assertThat(denseVector.references().names(), equalTo(Set.of(field.name())));
        as(denseVector.child(), EsRelation.class);
    }

    public void testProjectStaysBelowOrderBy() {
        EsRelation relation = relation();
        Project project = new Project(EMPTY, relation, List.of(new Alias(EMPTY, "renamed", relation.output().getFirst())));

        LogicalPlan plan = new HoistProjects().apply(new OrderBy(EMPTY, project, List.of()));

        as(as(plan, OrderBy.class).child(), Project.class);
    }

    public void testProjectStaysBelowMvExpand() {
        LogicalPlan plan = new HoistProjects().apply(substituted("""
            FROM test
            | RENAME first_name AS fn
            | MV_EXPAND fn
            """));

        as(plan.collect(MvExpand.class).getFirst().child(), Project.class);
    }

    public void testProjectStaysBelowAggregate() {
        LogicalPlan plan = new HoistProjects().apply(substituted("""
            FROM test
            | RENAME salary AS s
            | STATS m = MAX(s)
            """));

        as(plan.collect(Aggregate.class).getFirst().child(), Project.class);
    }

    public void testOnePassHoistsTheProjectPastEverySupportedNode() {
        hoistedOnce("""
            FROM test
            | RENAME first_name AS fn
            | WHERE fn IS NOT NULL
            | EVAL fn_length = LENGTH(fn)
            | DISSECT fn "%{head}"
            | GROK fn "%{WORD:word}"
            | RENAME languages AS lc
            | ENRICH languages_idx ON lc WITH language_name
            | LIMIT 10
            | EVAL language_code = lc
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS looked_up
            | DROP language_code
            """, Filter.class);
    }

    public void testOneRunHoistsTheProjectAboveEveryLookupBlock() {
        LogicalPlan plan = new HoistProjects().apply(substituted(lookupBlocks(3)));

        var project = as(plan, Project.class);
        as(project.child(), Limit.class);
        assertThat(plan.collect(Project.class), hasSize(1));
        List<Join> joins = plan.collect(Join.class);
        assertThat(joins, hasSize(3));
        assertThat(joins.stream().map(Join::left).toList(), everyItem(not(instanceOf(Project.class))));
        assertThat(Expressions.names(project.projections()), hasItems("lang0", "lang1", "lang2"));
    }

    public void testOneRunHoistsTheProjectPastEvalsBetweenLookupBlocks() {
        LogicalPlan plan = new HoistProjects().apply(substituted("""
            FROM test
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang
            | EVAL upper = TO_UPPER(lang)
            | EVAL language_code = languages + 1
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS next_lang
            """));

        as(plan, Project.class);
        assertThat(plan.collect(Project.class), hasSize(1));
        Join upperJoin = plan.collect(Join.class).getFirst();
        var eval = as(upperJoin.left(), Eval.class);
        assertThat(eval.collect(Project.class), hasSize(0));
    }

    public void testSecondApplicationChangesNothing() {
        LogicalPlan once = new HoistProjects().apply(substituted(lookupBlocks(3, "\n| WHERE lang%d IS NOT NULL")));

        assertThat(new HoistProjects().apply(once), equalTo(once));
    }

    public void testLocalVariantHoistsTheSameWay() {
        LogicalPlan substituted = substituted("""
            FROM test
            | RENAME first_name AS fn
            | WHERE fn IS NOT NULL
            | DISSECT fn "%{head}"
            | EVAL language_code = languages + 1
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang
            | ENRICH languages_idx ON languages WITH enriched = language_name
            | LIMIT 10
            """);

        assertThat(new HoistProjects().local(), instanceOf(HoistProjects.class));
        assertThat(new HoistProjects().local().apply(substituted), equalTo(new HoistProjects().apply(substituted)));
    }

    public void testOperatorBatchRunsDoNotGrowWithTheNumberOfLookupBlocks() {
        assertThat(operatorRunsToConverge(lookupBlocks(30)), equalTo(operatorRunsToConverge(lookupBlocks(3))));
    }

    public void testOperatorBatchRunsDoNotGrowWithFiltersOnLookedUpColumns() {
        String block = "\n| WHERE lang%d IS NOT NULL";
        assertThat(operatorRunsToConverge(lookupBlocks(30, block)), equalTo(operatorRunsToConverge(lookupBlocks(3, block))));
    }

    public void testOperatorBatchRunsDoNotGrowWithDissectBetweenLookupBlocks() {
        String block = "\n| DISSECT first_name \"%%{part%d}\"";
        assertThat(operatorRunsToConverge(lookupBlocks(30, block)), equalTo(operatorRunsToConverge(lookupBlocks(3, block))));
    }

    public void testOperatorBatchRunsDoNotGrowWithUriPartsBetweenLookupBlocks() {
        assumeTrue("requires uri_parts command capability", EsqlCapabilities.Cap.URI_PARTS_COMMAND.isEnabled());
        String block = "\n| URI_PARTS uri%d = first_name";
        assertThat(operatorRunsToConverge(lookupBlocks(30, block)), equalTo(operatorRunsToConverge(lookupBlocks(3, block))));
    }

    public void testOperatorBatchRunsDoNotGrowWithEnrichBetweenLookupBlocks() {
        String block = "\n| ENRICH languages_idx ON languages WITH enriched%d = language_name";
        assertThat(operatorRunsToConverge(lookupBlocks(30, block)), equalTo(operatorRunsToConverge(lookupBlocks(3, block))));
    }

    public void testLookupChainLongerThanTheRuleExecutionLimitOptimizes() {
        int blocks = 110;
        String query = lookupBlocks(blocks);
        LogicalPlan analyzed = defaultAnalyzer().query(query);

        LogicalPlan plan = logicalOptimizer.optimize(analyzed);

        as(plan, Project.class);
        assertThat(plan.collect(Project.class), hasSize(1));
        assertThat(plan.collect(Join.class), hasSize(blocks));
        assertThat(Expressions.names(plan.output()), equalTo(Expressions.names(analyzed.output())));
    }

    private <T extends LogicalPlan> T hoistedPast(String query, Class<T> nodeType) {
        return hoistedOnce(query, nodeType).collect(nodeType).getFirst();
    }

    private LogicalPlan hoistedOnce(String query, Class<? extends LogicalPlan> nodeType) {
        return hoistedOnce(substituted(query), nodeType);
    }

    private static LogicalPlan hoistedOnce(LogicalPlan before, Class<? extends LogicalPlan> nodeType) {
        assertTrue("the plan must have a Project directly below a " + nodeType.getSimpleName(), hasProjectChild(before, nodeType));

        LogicalPlan plan = new HoistProjects().apply(before);

        assertFalse(hasProjectChild(plan, nodeType));
        as(plan, Project.class);
        assertThat(plan.collect(Project.class), hasSize(1));
        assertThat(plan.output(), equalTo(before.output()));
        return plan;
    }

    private static boolean hasProjectChild(LogicalPlan plan, Class<? extends LogicalPlan> nodeType) {
        return plan.anyMatch(p -> nodeType.isInstance(p) && p.children().stream().anyMatch(c -> c instanceof Project));
    }

    private static Completion completion(LogicalPlan child, Attribute prompt, String targetName) {
        return new Completion(
            EMPTY,
            child,
            Literal.keyword(EMPTY, "inference-id"),
            new Literal(EMPTY, 1000, DataType.INTEGER),
            prompt,
            referenceAttribute(targetName, DataType.KEYWORD)
        );
    }

    private static EsRelation relation() {
        return new EsRelation(
            EMPTY,
            "test",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(),
            List.<Attribute>of(getFieldAttribute("field", DataType.KEYWORD))
        );
    }

    private static String lookupBlocks(int count) {
        return lookupBlocks(count, "");
    }

    private static String lookupBlocks(int count, String extraCommandFormat) {
        StringBuilder query = new StringBuilder("FROM test");
        for (int i = 0; i < count; i++) {
            query.append("\n| EVAL language_code = languages + ")
                .append(i)
                .append("\n| LOOKUP JOIN languages_lookup ON language_code\n| RENAME language_name AS lang")
                .append(i)
                .append(String.format(Locale.ROOT, extraCommandFormat, i))
                .append("\n| DROP language_code");
        }
        return query.toString();
    }

    private LogicalPlan substituted(String query) {
        return new TestSubstitutionOnlyOptimizer(logicalOptimizerCtx).optimize(defaultAnalyzer().query(query));
    }

    private int operatorRunsToConverge(String query) {
        LogicalPlan analyzed = defaultAnalyzer().query(query);
        for (int runs = 1; runs <= 100; runs++) {
            try {
                new OperatorRunsLimitedOptimizer(logicalOptimizerCtx, runs).optimize(analyzed);
                return runs;
            } catch (RuleExecutionException e) {
                assertThat(e.getMessage(), containsString("Rule execution limit [" + runs + "] reached"));
            }
        }
        throw new AssertionError("operator batch did not converge in 100 runs");
    }

    private static final class OperatorRunsLimitedOptimizer extends LogicalPlanOptimizer {
        private final int maxRuns;

        OperatorRunsLimitedOptimizer(LogicalOptimizerContext context, int maxRuns) {
            super(context);
            this.maxRuns = maxRuns;
        }

        @Override
        protected List<RuleExecutor.Batch<LogicalPlan>> batches() {
            String operatorsName = operators().name();
            List<RuleExecutor.Batch<LogicalPlan>> batches = new ArrayList<>();
            for (RuleExecutor.Batch<LogicalPlan> batch : super.batches()) {
                batches.add(
                    batch.name().equals(operatorsName)
                        ? new RuleExecutor.Batch<>(batch.name(), new RuleExecutor.Limiter(maxRuns), batch.rules())
                        : batch
                );
            }
            return batches;
        }
    }
}
