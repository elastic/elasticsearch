/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.PromoteSourceFanIn;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.elasticsearch.xpack.esql.view.ViewCompaction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class SourceFanInUnionAllTests extends ESTestCase {

    public void testCheckForkAllowsSourceFanIn() {
        Fork fork = new Fork(Source.EMPTY, List.of(fanIn(external("a"), external("b")), index("idx")), List.of());
        assertThat(verifyAnalysis(fork), not(containsString("FORK after subquery")));
        assertThat(verifyAnalysis(fork), not(containsString("Only a single FORK")));
    }

    public void testCheckForkRejectsUserUnionViewUnionAndSecondFork() {
        Fork overUnion = new Fork(
            Source.EMPTY,
            List.of(new UnionAll(Source.EMPTY, List.of(external("a"), external("b")), List.of()), index("idx")),
            List.of()
        );
        assertThat(verifyAnalysis(overUnion), containsString("FORK after subquery is not supported"));

        Fork overView = new Fork(Source.EMPTY, List.of(viewOf(external("a"), index("idx")), index("other")), List.of());
        assertThat(verifyAnalysis(overView), containsString("FORK after subquery is not supported"));

        Fork nested = new Fork(
            Source.EMPTY,
            List.of(new Fork(Source.EMPTY, List.of(index("a"), index("b")), List.of()), index("c")),
            List.of()
        );
        assertThat(verifyAnalysis(nested), containsString("Only a single FORK command is supported"));
    }

    public void testPromoteDatasetViewsAndMixedSourceBesideIndex() {
        LogicalPlan datasets = new PromoteSourceFanIn().apply(viewOf(external("ds1"), external("ds2")));
        assertThat(datasets, instanceOf(SourceFanInUnionAll.class));
        assertThat(datasets.children(), hasSize(2));

        LogicalPlan mixed = new PromoteSourceFanIn().apply(viewOf(external("ds"), index("idx")));
        assertThat(mixed, instanceOf(SourceFanInUnionAll.class));
        assertThat(mixed.children().get(0), instanceOf(ExternalRelation.class));
        assertThat(mixed.children().get(1), instanceOf(EsRelation.class));
    }

    public void testPromoteFlattensNestedFanInAndRejectsNinthProducer() {
        LogicalPlan flattened = new PromoteSourceFanIn().apply(viewOf(fanIn(external("a"), external("b")), external("c")));
        assertThat(flattened, instanceOf(SourceFanInUnionAll.class));
        assertThat(flattened.children(), hasSize(3));
        assertThat(flattened.children().get(0), instanceOf(ExternalRelation.class));

        List<LogicalPlan> nine = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            nine.add(external("ds" + i));
        }
        VerificationException tooMany = expectThrows(VerificationException.class, () -> new PromoteSourceFanIn().apply(viewOf(nine)));
        assertThat(tooMany.getMessage(), containsString("limit of " + SourceFanInUnionAll.MAX_PRODUCERS));
    }

    public void testPromoteLeavesIndexOnlyViewUnion() {
        ViewUnionAll indexes = viewOf(index("a"), index("b"));
        assertThat(new PromoteSourceFanIn().apply(indexes), equalTo(indexes));
    }

    public void testPromoteKeepsFilterOnFanInBesideNamesake() {
        SourceFanInUnionAll inner = fanIn(external("ds1"), external("ds2"));
        Filter filter = new Filter(Source.EMPTY, inner, new Literal(Source.EMPTY, true, DataType.BOOLEAN));
        LogicalPlan promoted = new PromoteSourceFanIn().apply(viewOf(filter, index("namesake")));

        assertThat(promoted, instanceOf(SourceFanInUnionAll.class));
        assertThat(promoted.children(), hasSize(2));
        assertThat(promoted.children().get(0), instanceOf(Filter.class));
        assertThat(((Filter) promoted.children().get(0)).child(), instanceOf(SourceFanInUnionAll.class));
        assertThat(((Filter) promoted.children().get(0)).child().children(), hasSize(2));
        assertThat(promoted.children().get(1), instanceOf(EsRelation.class));
    }

    public void testPromoteRejectsEightProducersPlusNamesake() {
        List<LogicalPlan> producers = new ArrayList<>();
        for (int i = 0; i < SourceFanInUnionAll.MAX_PRODUCERS; i++) {
            producers.add(external("ds" + i));
        }
        Filter filter = new Filter(Source.EMPTY, fanIn(producers), new Literal(Source.EMPTY, true, DataType.BOOLEAN));
        VerificationException tooMany = expectThrows(
            VerificationException.class,
            () -> new PromoteSourceFanIn().apply(viewOf(filter, index("namesake")))
        );
        assertThat(tooMany.getMessage(), containsString("limit of " + SourceFanInUnionAll.MAX_PRODUCERS));
    }

    public void testViewUnionOfForkBesideNamesakeIsNotPromoted() {
        ViewUnionAll view = viewOf(new Fork(Source.EMPTY, List.of(index("a"), index("b")), List.of()), index("namesake"));
        LogicalPlan promoted = new PromoteSourceFanIn().apply(view);
        assertThat(promoted, instanceOf(ViewUnionAll.class));

        Failures failures = new Failures();
        ((PostOptimizationPlanVerificationAware) promoted).postOptimizationPlanVerification().accept(promoted, failures);
        assertThat(failures.toString(), containsString("FORK inside subquery is not supported"));
    }

    public void testUserUnionOfFilteredFanInIsNotPromoted() {
        Filter filter = new Filter(
            Source.EMPTY,
            fanIn(external("ds1"), external("ds2")),
            new Literal(Source.EMPTY, true, DataType.BOOLEAN)
        );
        UnionAll userUnion = new UnionAll(Source.EMPTY, List.of(filter, index("idx")), List.of());
        LogicalPlan promoted = new PromoteSourceFanIn().apply(userUnion);
        assertThat(promoted, instanceOf(UnionAll.class));
        assertThat(promoted, not(instanceOf(SourceFanInUnionAll.class)));
    }

    public void testCompactionDoesNotLiftFanInOrFork() {
        ViewUnionAll nestedFanIn = viewOf(viewOf(fanIn(external("a"), external("b"))));
        LogicalPlan compactedFanIn = ViewCompaction.postIndexResolution(nestedFanIn);
        assertThat(compactedFanIn.anyMatch(p -> p instanceof SourceFanInUnionAll), equalTo(true));

        Fork fork = new Fork(Source.EMPTY, List.of(index("a"), index("b")), List.of());
        LogicalPlan compactedFork = ViewCompaction.postIndexResolution(viewOf(fork));
        assertThat(compactedFork.anyMatch(p -> p instanceof Fork), equalTo(true));
    }

    public void testNamedSubqueryRewriteLeavesSourceFanIn() {
        NamedSubquery named = new NamedSubquery(Source.EMPTY, external("a"), "v");
        SourceFanInUnionAll fanIn = fanIn(named, external("b"));
        assertThat(ViewCompaction.preIndexResolution(fanIn), instanceOf(SourceFanInUnionAll.class));
    }

    public void testStripViewShadowCollapsesFanIn() {
        ViewShadowRelation shadow = new ViewShadowRelation(Source.EMPTY, "v", LinkedIndexPattern.Kind.OPTIONAL, "v");
        SourceFanInUnionAll fanIn = fanIn(external("a"), shadow);
        assertThat(ViewCompaction.postIndexResolution(fanIn), instanceOf(ExternalRelation.class));
    }

    public void testBranchCapDoesNotApplyToWideFanIn() {
        List<LogicalPlan> nine = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            nine.add(external("ds" + i));
        }
        SourceFanInUnionAll fanIn = fanIn(nine);

        Failures branchCap = new Failures();
        MergePlan.checkBranchCount(fanIn, branchCap);
        assertThat(branchCap.toString(), branchCap.hasFailures(), equalTo(false));

        // The verifier applies every plan checker to every node, so FORK's checker also sees the fan-in.
        Fork fork = new Fork(Source.EMPTY, List.of(fanIn, index("other")), List.of());
        Failures broadcast = new Failures();
        fork.postAnalysisPlanVerification().accept(fanIn, broadcast);
        assertThat(broadcast.toString(), not(containsString("FORK supports up to")));

        Failures own = new Failures();
        fanIn.postAnalysisPlanVerification().accept(fanIn, own);
        assertThat(own.toString(), containsString("resolved to 9 sources"));

        List<LogicalPlan> forkBranches = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            forkBranches.add(index("b" + i));
        }
        Fork wideFork = new Fork(Source.EMPTY, forkBranches, List.of());
        Failures forkCap = new Failures();
        wideFork.postAnalysisPlanVerification().accept(wideFork, forkCap);
        assertThat(forkCap.toString(), containsString("FORK supports up to"));
    }

    public void testEightDatasetsPlusEightNamesakesFailProducerCheck() {
        List<LogicalPlan> children = new ArrayList<>();
        for (int i = 0; i < SourceFanInUnionAll.MAX_PRODUCERS; i++) {
            children.add(external("ds" + i));
            children.add(index("idx" + i));
        }
        SourceFanInUnionAll fanIn = fanIn(children);
        Failures failures = new Failures();
        fanIn.postAnalysisPlanVerification().accept(fanIn, failures);
        assertThat(failures.toString(), containsString("resolved to 16 sources"));
        assertThat(failures.toString(), containsString("limit of " + SourceFanInUnionAll.MAX_PRODUCERS));
    }

    public void testForkBranchesTimesProducersFailQueryWideCap() {
        List<LogicalPlan> branches = new ArrayList<>();
        for (int i = 0; i < SourceFanInUnionAll.MAX_PRODUCERS; i++) {
            List<LogicalPlan> producers = new ArrayList<>();
            for (int j = 0; j < SourceFanInUnionAll.MAX_PRODUCERS; j++) {
                producers.add(external("d" + i + "_" + j));
            }
            branches.add(fanIn(producers));
        }
        Fork fork = new Fork(Source.EMPTY, branches, List.of());
        Failures failures = new Failures();
        UnionAll.checkForkSourceFanInLeafCount(fork, 20, failures);
        assertThat(failures.toString(), containsString("64 branches"));
        assertThat(failures.toString(), containsString("limit of 20"));

        Failures snapshot = new Failures();
        UnionAll.checkNestedSubqueryLimits(fork, 20, 5, snapshot);
        assertThat(snapshot.toString(), containsString("64 branches"));
    }

    public void testReleaseCheckIgnoresUnrelatedIndexUnionAndInSubquery() {
        List<LogicalPlan> indexes = new ArrayList<>();
        for (int i = 0; i < 21; i++) {
            indexes.add(index("idx" + i));
        }
        FieldAttribute left = field("emp_no");
        FieldAttribute right = field("emp_no");
        Fork smallFork = new Fork(Source.EMPTY, List.of(fanIn(external("c", left), external("d", left)), index("only", left)), List.of());
        LogicalPlan plan = new SemiJoin(
            Source.EMPTY,
            new UnionAll(Source.EMPTY, List.of(smallFork, viewOf(indexes)), List.of()),
            fanIn(external("a", right), external("b", right)),
            List.of(left),
            List.of(right)
        );
        Failures failures = new Failures();
        UnionAll.checkForkSourceFanInLeafCount(plan, 20, failures);
        assertThat(failures.toString(), failures.hasFailures(), equalTo(false));
    }

    private static String verifyAnalysis(LogicalPlan plan) {
        Failures failures = new Failures();
        ((PostAnalysisPlanVerificationAware) plan).postAnalysisPlanVerification().accept(plan, failures);
        return failures.toString();
    }

    private static SourceFanInUnionAll fanIn(LogicalPlan... children) {
        return fanIn(List.of(children));
    }

    private static SourceFanInUnionAll fanIn(List<LogicalPlan> children) {
        return new SourceFanInUnionAll(Source.EMPTY, children, List.of());
    }

    private static ViewUnionAll viewOf(LogicalPlan... children) {
        return viewOf(List.of(children));
    }

    private static ViewUnionAll viewOf(List<LogicalPlan> children) {
        LinkedHashMap<String, LogicalPlan> named = new LinkedHashMap<>();
        for (int i = 0; i < children.size(); i++) {
            named.put("b" + i, children.get(i));
        }
        return new ViewUnionAll(Source.EMPTY, named, List.of());
    }

    private static ExternalRelation external(String name) {
        return external(name, field("emp_no"));
    }

    private static ExternalRelation external(String name, Attribute attribute) {
        List<Attribute> attributes = List.of(attribute);
        SourceMetadata metadata = new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return attributes;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String location() {
                return "s3://bucket/" + name;
            }

            @Override
            public boolean equals(Object o) {
                return o instanceof SourceMetadata other && location().equals(other.location());
            }

            @Override
            public int hashCode() {
                return location().hashCode();
            }
        };
        return new ExternalRelation(Source.EMPTY, "s3://bucket/" + name, metadata, attributes, FileList.UNRESOLVED, Map.of(), name);
    }

    private static EsRelation index(String name) {
        return index(name, field("emp_no"));
    }

    private static EsRelation index(String name, Attribute attribute) {
        return new EsRelation(Source.EMPTY, name, IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of(attribute));
    }

    private static FieldAttribute field(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
    }
}
