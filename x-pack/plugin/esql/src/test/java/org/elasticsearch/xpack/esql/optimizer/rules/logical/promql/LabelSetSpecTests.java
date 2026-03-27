/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class LabelSetSpecTests extends ESTestCase {

    static final Attribute CLUSTER = attr("cluster");
    static final Attribute POD = attr("pod");
    static final Attribute REGION = attr("region");
    static final Attribute OTHER = attr("other");

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }

    private static Set<String> names(LabelSetSpec spec) {
        return spec.declared().stream().map(Attribute::name).collect(Collectors.toSet());
    }

    private static Set<String> excludedNames(LabelSetSpec spec) {
        return spec.excluded().stream().map(Attribute::name).collect(Collectors.toSet());
    }

    private static Set<String> applyMissingNames(LabelSetSpec spec, List<Attribute> available) {
        return spec.withIncluded(available).apply().missingAttributes().stream().map(Attribute::name).collect(Collectors.toSet());
    }

    // -- of: create exact label set --

    public void testOf() {
        var result = LabelSetSpec.of(List.of(CLUSTER, POD));

        assertThat(names(result), equalTo(Set.of("cluster", "pod")));
        assertThat(result.excluded(), empty());
    }

    // -- by: intersection with input. Kills uncertainty. --

    public void testByIntersectsApplyInput() {
        var input = LabelSetSpec.of(List.of(CLUSTER, POD, REGION));
        var result = LabelSetSpec.by(input, List.of(CLUSTER, POD));

        assertThat(names(result), equalTo(Set.of("cluster", "pod")));
    }

    public void testByApplyEmptyInput() {
        var input = LabelSetSpec.none();
        var result = LabelSetSpec.by(input, List.of(CLUSTER));

        assertThat(result.declared(), empty());
        assertThat(applyMissingNames(result, List.of(OTHER)), equalTo(Set.of("cluster")));
    }

    public void testByApplyEmptyDeclared() {
        var input = LabelSetSpec.of(List.of(CLUSTER, POD));
        var result = LabelSetSpec.by(input, List.of());

        assertThat(result.declared(), empty());
    }

    // -- without: remove declared, introduce uncertainty --

    public void testWithoutFromExact() {
        var input = LabelSetSpec.of(List.of(CLUSTER, POD, REGION));
        var result = LabelSetSpec.without(input, List.of(POD));

        assertThat(names(result), equalTo(Set.of("cluster", "region")));
        assertThat(excludedNames(result), equalTo(Set.of("pod")));
    }

    public void testWithoutFromWithout() {
        var input = LabelSetSpec.without(LabelSetSpec.of(List.of(CLUSTER, REGION)), List.of(POD));
        var result = LabelSetSpec.without(input, List.of(REGION));

        assertThat(names(result), equalTo(Set.of("cluster")));
        assertThat(excludedNames(result), equalTo(Set.of("pod", "region")));
    }

    public void testWithoutEmptyExclusion() {
        var input = LabelSetSpec.of(List.of(CLUSTER, REGION));
        var result = LabelSetSpec.without(input, List.of());
        assertThat(names(result), equalTo(Set.of("cluster", "region")));
    }

    public void testWithoutAllDeclared() {
        var input = LabelSetSpec.of(List.of(CLUSTER, POD));
        var result = LabelSetSpec.without(input, List.of(CLUSTER, POD));

        assertThat(result.declared(), empty());
        assertThat(excludedNames(result), equalTo(Set.of("cluster", "pod")));
    }

    public void testWithoutFromEmpty() {
        var input = LabelSetSpec.none();
        var result = LabelSetSpec.without(input, List.of(POD));

        assertThat(result.declared(), empty());
        assertThat(excludedNames(result), equalTo(Set.of("pod")));
    }

    // -- none --

    public void testNone() {
        var result = LabelSetSpec.none();

        assertThat(result.declared(), empty());
    }

    // -- Composition: by then without --

    public void testByThenWithout() {
        var input = LabelSetSpec.of(List.of(CLUSTER, POD, REGION));
        var afterBy = LabelSetSpec.by(input, List.of(CLUSTER, POD, REGION));
        var result = LabelSetSpec.without(afterBy, List.of(POD));

        assertThat(names(result), equalTo(Set.of("cluster", "region")));
        assertThat(excludedNames(result), equalTo(Set.of("pod")));
    }

    // -- Composition: without then by --

    public void testWithoutThenBy() {
        var input = LabelSetSpec.of(List.of(CLUSTER, POD, REGION));
        var afterWithout = LabelSetSpec.without(input, List.of(POD));
        var result = LabelSetSpec.by(afterWithout, List.of(CLUSTER));

        assertThat(names(result), equalTo(Set.of("cluster")));
        assertThat(result.excluded(), empty());
    }

    public void testWithoutThenByExcludedLabel() {
        var input = LabelSetSpec.of(List.of(CLUSTER, POD, REGION));
        var afterWithout = LabelSetSpec.without(input, List.of(POD));
        var result = LabelSetSpec.by(afterWithout, List.of(POD));

        assertThat(result.declared(), empty());
        assertThat(result.excluded(), empty());
        assertThat(applyMissingNames(result, List.of(OTHER)), equalTo(Set.of("pod")));
    }

    // -- Composition: by then by --

    public void testByThenBy() {
        var input = LabelSetSpec.of(List.of(CLUSTER, POD, REGION));
        var afterBy1 = LabelSetSpec.by(input, List.of(CLUSTER, POD));
        var result = LabelSetSpec.by(afterBy1, List.of(CLUSTER));

        assertThat(names(result), equalTo(Set.of("cluster")));
    }

    // -- Composition: without then without --

    public void testWithoutThenWithoutAccumulatesExclusions() {
        var input = LabelSetSpec.of(List.of(CLUSTER, POD, REGION));
        var afterFirst = LabelSetSpec.without(input, List.of(POD));
        assertThat(excludedNames(afterFirst), equalTo(Set.of("pod")));

        var result = LabelSetSpec.without(afterFirst, List.of(REGION));

        assertThat(names(result), equalTo(Set.of("cluster")));
        assertThat(excludedNames(result), equalTo(Set.of("pod", "region")));
    }

    // -- intersectWithLabels --

    public void testIntersectApplyDeclaredPassesThroughWhenEmpty() {
        var input = LabelSetSpec.without(LabelSetSpec.of(List.of(CLUSTER)), List.of(POD));
        var result = LabelSetSpec.intersectWithLabels(input, List.of());

        assertThat(names(result), equalTo(Set.of("cluster")));
    }

    public void testIntersectApplyDeclaredIntersects() {
        var input = LabelSetSpec.of(List.of(CLUSTER, POD));
        var result = LabelSetSpec.intersectWithLabels(input, List.of(CLUSTER, REGION));

        assertThat(names(result), equalTo(Set.of("cluster")));
    }

    // -- Idempotence --

    public void testWithoutIdempotent() {
        var input = LabelSetSpec.of(List.of(CLUSTER, POD, REGION));
        var once = LabelSetSpec.without(input, List.of(POD));
        var twice = LabelSetSpec.without(once, List.of(POD));
        assertThat(names(twice), equalTo(names(once)));
    }

    public void testByIdempotent() {
        var input = LabelSetSpec.of(List.of(CLUSTER, POD, REGION));
        var once = LabelSetSpec.by(input, List.of(CLUSTER));
        var twice = LabelSetSpec.by(once, List.of(CLUSTER));
        assertThat(names(twice), equalTo(names(once)));
    }

    // -- apply: excluded dimension resolution --

    public void testApplyExcludedGroupingsCombinesInheritedExcluded() {
        Attribute dimPod = dimensionAttr("pod");
        Attribute dimRegion = dimensionAttr("region");

        var child = LabelSetSpec.without(LabelSetSpec.of(List.of(dimPod, dimRegion)), List.of(dimPod));
        var result = child.withExcluded(List.of(dimRegion)).apply();

        assertThat(result.excludedGroupings().stream().map(Attribute::name).collect(Collectors.toSet()), equalTo(Set.of("pod", "region")));
    }

    public void testApplyExcludedGroupingsFiltersNonDimensions() {
        var child = LabelSetSpec.without(LabelSetSpec.of(List.of(POD)), List.of(POD));
        var result = child.withExcluded(List.of()).apply();

        assertThat(result.excludedGroupings(), empty());
    }

    public void testApplyExcludedGroupingsDeduplicates() {
        Attribute dimPod = dimensionAttr("pod");

        var child = LabelSetSpec.without(LabelSetSpec.of(List.of(dimPod)), List.of(dimPod));
        var result = child.withExcluded(List.of(dimPod)).apply();

        assertThat(result.excludedGroupings().size(), equalTo(1));
        assertThat(result.excludedGroupings().getFirst().name(), equalTo("pod"));
    }

    private static Attribute dimensionAttr(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            name,
            new EsField(name, DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION)
        );
    }
}
