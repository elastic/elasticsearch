/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.promql.UnresolvedPromqlLabelRef;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

/**
 * Tracing names metadata after what it leaves out of the source's series; key selection carries the labels a grouping
 * boundary must keep. Plan-level binding is covered by the PromQL plan and golden tests.
 */
public class ResolvePromqlLabelRefsTests extends ESTestCase {
    private static final List<String> LABELS = List.of("pod", "event", "unmentioned_dimension");

    /** An Eval stating metadata names the metadata above it after everything it leaves out of the source's series. */
    public void testStatementNamesMetadataAbove() {
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES);
        Alias derived = new Alias(Source.EMPTY, "dst", Literal.keyword(Source.EMPTY, "value"));
        LogicalPlan eval = new Eval(Source.EMPTY, relation, List.of(derived, statement(Set.of("dst"))));

        Project project = as(ResolvePromqlLabelRefs.trace(read(eval)), Project.class);
        Eval rest = as(project.child(), Eval.class);
        assertEquals(List.of(derived), rest.fields());
        assertSame(relation, rest.child());
        assertRead(project, Set.of("dst"), "_timeseries$dst");
    }

    /** Each statement leaves its labels out of the metadata the one before it states. */
    public void testStatementsComposeInFieldOrder() {
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES);
        LogicalPlan eval = new Eval(Source.EMPTY, relation, List.of(statement(Set.of("pod")), statement(Set.of("dst"))));

        Project project = as(ResolvePromqlLabelRefs.trace(read(eval)), Project.class);
        assertSame(relation, project.child());
        assertRead(project, Set.of("pod", "dst"), "_timeseries$dst$pod");
    }

    /**
     * Every grouping boundary over three labels: each subset declared and wanted, metadata declared or not, up to two
     * metadata views wanted, and every initial and per-series mode. An independent oracle picks the metadata view a
     * source must read: the one retaining the least information from which every wanted view derives.
     */
    public void testKeySelectionOverAllBoundedGrains() {
        List<List<Integer>> viewDemands = new ArrayList<>();
        viewDemands.add(List.of());
        for (int a = 0; a < 8; a++) {
            viewDemands.add(List.of(a));
            for (int b = 0; b < 8; b++) {
                viewDemands.add(List.of(a, b));
            }
        }
        long checked = 0;
        for (int declaredView = -1; declaredView < 8; declaredView++) {
            for (int declaredLabels = 0; declaredLabels < 8; declaredLabels++) {
                for (int wantedLabels = 0; wantedLabels < 8; wantedLabels++) {
                    for (List<Integer> wantedViews : viewDemands) {
                        for (int mode = 0; mode < 4; mode++) {
                            boolean initial = (mode & 1) != 0;
                            boolean perSeries = (mode & 2) != 0;
                            var grain = new ResolvePromqlLabelRefs.Grain(
                                declaredView < 0 ? null : view(declaredView),
                                labels(declaredLabels),
                                perSeries
                            );
                            var needs = new ArrayList<UnresolvedPromqlLabelRef>();
                            wantedViews.forEach(v -> needs.add(view(v)));
                            needs.addAll(labels(wantedLabels));
                            boolean source = initial && perSeries;
                            var keys = ResolvePromqlLabelRefs.selectKeys(grain, needs, initial);
                            int expectedView = expectedView(declaredView, wantedViews, source);
                            assertEquals(
                                expectedView < 0 ? null : names(expectedView),
                                keys.metadata() == null ? null : keys.metadata().unset()
                            );
                            var expectedLabels = new HashSet<>(names(declaredLabels));
                            for (String label : names(wantedLabels)) {
                                if (source || (declaredView >= 0 && names(declaredView).contains(label) == false)) {
                                    expectedLabels.add(label);
                                }
                            }
                            var actualLabels = keys.labels().stream().map(UnresolvedPromqlLabelRef::name).toList();
                            assertEquals(expectedLabels, new HashSet<>(actualLabels));
                            assertEquals(expectedLabels.size(), actualLabels.size());
                            checked++;
                        }
                    }
                }
            }
        }
        assertEquals(168192L, checked);
    }

    /**
     * The view a source reads: the largest set of exclusions contained in every requested view. A source keeps one row per
     * series whatever view it reads, so a declared grain nobody reads is no requirement.
     */
    private static int expectedView(int declared, List<Integer> requested, boolean source) {
        if (source == false || requested.isEmpty()) {
            return declared;
        }
        int best = 0;
        for (int candidate = 0; candidate < 8; candidate++) {
            boolean derivesAll = true;
            for (int desired : requested) {
                // a view derives another only by leaving more labels out
                if ((candidate & desired) != candidate) {
                    derivesAll = false;
                }
            }
            if (derivesAll && Integer.bitCount(candidate) > Integer.bitCount(best)) {
                best = candidate;
            }
        }
        return best;
    }

    private static Alias statement(Set<String> unset) {
        return new Alias(Source.EMPTY, MetadataAttribute.TIMESERIES, UnresolvedPromqlLabelRef.metadata(Source.EMPTY, unset));
    }

    /** A projection reading the metadata of {@code child} as is. */
    private static Project read(LogicalPlan child) {
        return new Project(Source.EMPTY, child, List.of(statement(Set.of())));
    }

    private static void assertRead(Project project, Set<String> unset, String name) {
        Alias projected = as(project.projections().getFirst(), Alias.class);
        UnresolvedPromqlLabelRef ref = as(projected.child(), UnresolvedPromqlLabelRef.class);
        assertEquals(unset, ref.unset());
        assertEquals(name, ref.name());
    }

    private static UnresolvedPromqlLabelRef view(int excluded) {
        return UnresolvedPromqlLabelRef.metadata(Source.EMPTY, names(excluded)).againstSource(names(excluded));
    }

    private static List<UnresolvedPromqlLabelRef> labels(int mask) {
        return LABELS.stream()
            .filter(name -> names(mask).contains(name))
            .map(name -> UnresolvedPromqlLabelRef.label(Source.EMPTY, name))
            .toList();
    }

    private static Set<String> names(int mask) {
        var names = new HashSet<String>();
        for (int i = 0; i < LABELS.size(); i++) {
            if ((mask & (1 << i)) != 0) {
                names.add(LABELS.get(i));
            }
        }
        return names;
    }
}
