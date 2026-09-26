/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.PromoteSourceFanIn;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.view.ViewCompaction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        // A dataset view is one FROM, so it is allowed under FORK the same way a source fan-in is.
        Fork overView = new Fork(Source.EMPTY, List.of(viewOf(external("a"), index("idx")), index("other")), List.of());
        assertThat(verifyAnalysis(overView), not(containsString("FORK after subquery")));

        Fork overIndexView = new Fork(Source.EMPTY, List.of(viewOf(index("a"), index("b")), index("other")), List.of());
        assertThat(verifyAnalysis(overIndexView), containsString("FORK after subquery is not supported"));

        Fork nested = new Fork(
            Source.EMPTY,
            List.of(new Fork(Source.EMPTY, List.of(index("a"), index("b")), List.of()), index("c")),
            List.of()
        );
        assertThat(verifyAnalysis(nested), containsString("Only a single FORK command is supported"));
    }

    public void testPromoteDatasetViewsAndMixedSourceBesideIndex() {
        LogicalPlan datasets = PromoteSourceFanIn.promote(viewOf(external("ds1"), external("ds2")));
        assertThat(datasets, instanceOf(SourceFanInUnionAll.class));
        assertThat(datasets.children(), hasSize(2));

        LogicalPlan mixed = PromoteSourceFanIn.promote(viewOf(external("ds"), index("idx")));
        assertThat(mixed, instanceOf(SourceFanInUnionAll.class));
        assertThat(mixed.children().get(0), instanceOf(ExternalRelation.class));
        assertThat(mixed.children().get(1), instanceOf(EsRelation.class));
    }

    public void testPromoteFlattensNestedFanInAndRejectsNinthProducer() {
        LogicalPlan flattened = PromoteSourceFanIn.promote(viewOf(fanIn(external("a"), external("b")), external("c")));
        assertThat(flattened, instanceOf(SourceFanInUnionAll.class));
        assertThat(flattened.children(), hasSize(3));
        assertThat(flattened.children().get(0), instanceOf(ExternalRelation.class));

        List<LogicalPlan> nine = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            nine.add(external("ds" + i));
        }
        LogicalPlan tooMany = PromoteSourceFanIn.promote(viewOf(nine));
        assertThat(verifyAnalysis(tooMany), containsString("limit of " + SourceFanInUnionAll.MAX_PRODUCERS));
    }

    public void testPromoteLeavesIndexOnlyViewUnion() {
        ViewUnionAll indexes = viewOf(index("a"), index("b"));
        assertThat(PromoteSourceFanIn.promote(indexes), equalTo(indexes));
    }

    public void testPromoteKeepsFilterOnFanInBesideNamesake() {
        SourceFanInUnionAll inner = fanIn(external("ds1"), external("ds2"));
        Filter filter = new Filter(Source.EMPTY, inner, new Literal(Source.EMPTY, true, DataType.BOOLEAN));
        LogicalPlan promoted = PromoteSourceFanIn.promote(viewOf(filter, index("namesake")));

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
        LogicalPlan tooMany = PromoteSourceFanIn.promote(viewOf(filter, index("namesake")));
        assertThat(verifyAnalysis(tooMany), containsString("limit of " + SourceFanInUnionAll.MAX_PRODUCERS));
    }

    public void testViewUnionOfForkBesideNamesakeIsNotPromoted() {
        ViewUnionAll view = viewOf(new Fork(Source.EMPTY, List.of(index("a"), index("b")), List.of()), index("namesake"));
        LogicalPlan promoted = PromoteSourceFanIn.promote(view);
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
        LogicalPlan promoted = PromoteSourceFanIn.promote(userUnion);
        assertThat(promoted, instanceOf(UnionAll.class));
        assertThat(promoted, not(instanceOf(SourceFanInUnionAll.class)));
    }

    public void testCompactionDoesNotLiftFanInOrFork() {
        ViewUnionAll nestedFanIn = viewOf(viewOf(fanIn(external("a"), external("b"))));
        LogicalPlan compactedFanIn = ViewCompaction.postIndexResolution(nestedFanIn, false);
        assertThat(compactedFanIn.anyMatch(p -> p instanceof SourceFanInUnionAll), equalTo(true));

        Fork fork = new Fork(Source.EMPTY, List.of(index("a"), index("b")), List.of());
        LogicalPlan compactedFork = ViewCompaction.postIndexResolution(viewOf(fork), false);
        assertThat(compactedFork.anyMatch(p -> p instanceof Fork), equalTo(true));
    }

    /** A FORK view read beside another source cannot run nested, so compaction lifts its branches as it did before. */
    public void testCompactionLiftsForkBesideOtherSource() {
        Fork fork = new Fork(Source.EMPTY, List.of(index("a"), index("b")), List.of());
        LogicalPlan compacted = ViewCompaction.postIndexResolution(viewOf(fork, index("other")), false);
        assertThat(compacted.anyMatch(p -> p instanceof Fork), equalTo(false));
        assertThat(compacted.children(), hasSize(3));
    }

    /** A view union that is not promoted must not keep a fan-in nested under it; each producer becomes its own branch. */
    public void testPromoteLiftsFanInBesideNonPromotableBranch() {
        Filter filteredIndex = new Filter(Source.EMPTY, index("idx"), new Literal(Source.EMPTY, true, DataType.BOOLEAN));
        LogicalPlan promoted = PromoteSourceFanIn.promote(viewOf(fanIn(external("a"), external("b")), filteredIndex));

        assertThat(promoted, instanceOf(ViewUnionAll.class));
        ViewUnionAll view = (ViewUnionAll) promoted;
        assertThat(view.anyMatch(p -> p instanceof SourceFanInUnionAll), equalTo(false));
        assertThat(view.namedSubqueries().keySet().stream().toList(), equalTo(List.of("b0#1", "b0#2", "b1")));
        assertThat(view.viewBranchKeys(), equalTo(Set.of("b1")));
    }

    /**
     * With a request filter, a view branch that computes over its sources keeps its view boundary so the filter still
     * lands on the view output. A branch that is only sources under {@code WHERE} is still promoted.
     */
    public void testPreserveViewBoundariesPromotesOnlySourceOnlyViewBranches() {
        Limit limited = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 1, DataType.INTEGER), fanIn(external("a"), external("b")));
        assertThat(PromoteSourceFanIn.promote(viewOf(limited, index("namesake")), true), instanceOf(ViewUnionAll.class));
        assertThat(PromoteSourceFanIn.promote(viewOf(limited, index("namesake")), false), instanceOf(SourceFanInUnionAll.class));

        Filter filtered = new Filter(Source.EMPTY, fanIn(external("a"), external("b")), new Literal(Source.EMPTY, true, DataType.BOOLEAN));
        assertThat(PromoteSourceFanIn.promote(viewOf(filtered, index("namesake")), true), instanceOf(SourceFanInUnionAll.class));
    }

    public void testNamedSubqueryRewriteLeavesSourceFanIn() {
        NamedSubquery named = new NamedSubquery(Source.EMPTY, external("a"), "v");
        SourceFanInUnionAll fanIn = fanIn(named, external("b"));
        assertThat(ViewCompaction.preIndexResolution(fanIn), instanceOf(SourceFanInUnionAll.class));
    }

    public void testStripViewShadowCollapsesFanIn() {
        ViewShadowRelation shadow = new ViewShadowRelation(Source.EMPTY, "v", LinkedIndexPattern.Kind.OPTIONAL, "v");
        SourceFanInUnionAll fanIn = fanIn(external("a"), shadow);
        assertThat(ViewCompaction.postIndexResolution(fanIn, false), instanceOf(ExternalRelation.class));
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
        SourceFanInUnionAll fanIn = fanIn(children).withIndexReadsCollapsed();
        // The 8 index reads merge into one, leaving 8 datasets + 1 merged index = 9 producers.
        assertThat(fanIn.children(), hasSize(SourceFanInUnionAll.MAX_PRODUCERS + 1));
        Failures failures = new Failures();
        fanIn.postAnalysisPlanVerification().accept(fanIn, failures);
        assertThat(failures.toString(), containsString("resolved to 9 sources"));
        assertThat(failures.toString(), containsString("limit of " + SourceFanInUnionAll.MAX_PRODUCERS));
    }

    public void testSevenDatasetsPlusIndexReadsCollapseToEightBranches() {
        List<LogicalPlan> children = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            children.add(external("ds" + i));
        }
        children.add(index("idx"));
        for (int i = 0; i < 7; i++) {
            children.add(index("shadow" + i));
        }
        SourceFanInUnionAll fanIn = fanIn(children).withIndexReadsCollapsed();
        assertThat(fanIn.children(), hasSize(8));
        long indexBranches = fanIn.children().stream().filter(c -> c instanceof EsRelation).count();
        assertThat(indexBranches, equalTo(1L));
        EsRelation merged = (EsRelation) fanIn.children().get(7);
        assertThat(merged.indexPattern(), equalTo("idx,shadow0,shadow1,shadow2,shadow3,shadow4,shadow5,shadow6"));
        Failures failures = new Failures();
        fanIn.postAnalysisPlanVerification().accept(fanIn, failures);
        assertThat(failures.toString(), not(containsString("resolved to")));
    }

    public void testIndexReadsCountAsOneProducer() {
        List<LogicalPlan> children = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            children.add(external("ds" + i));
            children.add(index("shadow" + i));
        }
        children.add(index("idx"));
        LogicalPlan promoted = PromoteSourceFanIn.promote(fanIn(children));
        assertThat(promoted.children(), hasSize(5));
        assertThat(verifyAnalysis(promoted), not(containsString("resolved to")));
    }

    public void testConstructorKeepsIndexReads() {
        SourceFanInUnionAll fanIn = fanIn(external("ds"), index("a"), index("b"));
        assertThat(fanIn.children(), hasSize(3));
    }

    public void testIndexReadsWithConflictingTypesStaySeparate() {
        EsRelation keyword = index("a", field("emp_no", DataType.KEYWORD));
        EsRelation integer = index("b", field("emp_no", DataType.INTEGER));
        SourceFanInUnionAll fanIn = fanIn(external("ds"), keyword, integer);
        assertThat(fanIn.withIndexReadsCollapsed(), equalTo(fanIn));
    }

    /** Two reads of the same concrete index are two copies of its rows under UNION ALL, so they must not merge. */
    public void testIndexReadsOverSameConcreteIndexStaySeparate() {
        EsRelation first = indexOver("v_idx", "idx");
        EsRelation second = indexOver("idx", "idx");
        SourceFanInUnionAll overlapping = fanIn(external("ds"), first, second);
        assertThat(overlapping.withIndexReadsCollapsed(), equalTo(overlapping));

        SourceFanInUnionAll disjoint = fanIn(external("ds"), indexOver("a", "a"), indexOver("b", "b")).withIndexReadsCollapsed();
        assertThat(disjoint.children(), hasSize(2));
    }

    public void testIndexReadsWithDifferentMetadataStaySeparate() {
        FieldAttribute empNo = field("emp_no");
        MetadataAttribute id = new MetadataAttribute(Source.EMPTY, "_id", DataType.KEYWORD, false);
        EsRelation withId = new EsRelation(Source.EMPTY, "a", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of(empNo, id));
        SourceFanInUnionAll fanIn = fanIn(external("ds"), withId, index("b", empNo));
        assertThat(fanIn.withIndexReadsCollapsed(), equalTo(fanIn));
    }

    public void testEmptyMappingMarkerDroppedWhenMerged() {
        EsRelation empty = new EsRelation(Source.EMPTY, "empty", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), Analyzer.NO_FIELDS);
        FieldAttribute empNo = field("emp_no");
        SourceFanInUnionAll fanIn = fanIn(external("ds"), index("idx", empNo), empty).withIndexReadsCollapsed();
        assertThat(fanIn.children(), hasSize(2));
        assertThat(fanIn.children().get(1).output(), equalTo(List.of(empNo)));

        EsRelation otherEmpty = new EsRelation(Source.EMPTY, "other", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), Analyzer.NO_FIELDS);
        SourceFanInUnionAll bothEmpty = fanIn(external("ds"), empty, otherEmpty).withIndexReadsCollapsed();
        assertThat(bothEmpty.children().get(1).output(), equalTo(Analyzer.NO_FIELDS));
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
        UnionAll.checkNestedSubqueryLimits(fork, 20, 5, "[max_branch_count] query pragma", "[max_branch_level] query pragma", failures);
        assertThat(failures.toString(), containsString("64 branches"));
        assertThat(failures.toString(), containsString("limit of 20"));
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
        return new ViewUnionAll(Source.EMPTY, named, named.keySet(), List.of());
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

    private static EsRelation indexOver(String pattern, String concreteIndex) {
        return new EsRelation(
            Source.EMPTY,
            pattern,
            IndexMode.STANDARD,
            Map.of("", List.of(pattern)),
            Map.of("", List.of(concreteIndex)),
            Map.of(),
            List.of(field("emp_no"))
        );
    }

    private static FieldAttribute field(String name) {
        return field(name, DataType.INTEGER);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }
}
