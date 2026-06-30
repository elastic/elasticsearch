/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.InheritedAttributes;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.ResolvedAttributes;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.SynthesizedAttributes;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Exercises {@link PromqlAttributesTranslationContext} the way {@link TranslatePromqlToEsqlPlan} drives it for a nested
 * aggregation: the inherited {@link InheritedAttributes} flows down from the outermost aggregate to the leaf selector, then
 * the synthesized {@link SynthesizedAttributes} folds back up. Each test builds one aggregation hierarchy by hand so both
 * passes stay visible, and only asserts the label sets that actually reach the engine: the innermost
 * {@link ResolvedAttributes} (the single physical {@code _timeseries} grouping / TimeSeriesWithout) and the outermost
 * {@link ResolvedAttributes} (final grouping keys plus the BY labels that must be null-filled).
 */
public class PromqlAttributesTranslationContextTests extends ESTestCase {

    static final Attribute CLUSTER = attr("cluster");
    static final Attribute POD = attr("pod");
    static final Attribute REGION = attr("region");

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }

    private static Set<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).collect(Collectors.toSet());
    }

    /**
     * Exercises the label algebra for a {@code WITHOUT} stacked on another {@code WITHOUT}. Note this shape is rejected
     * at runtime by {@code PromqlCommand} validation ("nested WITHOUT over WITHOUT is not supported at this time"), so it
     * is not an executable query; the case is kept to pin down how the algebra accumulates exclusions across the chain.
     */
    public void testSumWithoutOverAvgWithout() {
        // sum without(pod) ( avg without(region) ( cpu_util ) )

        // descend: each aggregate narrows the demand handed to its child
        InheritedAttributes demandForAvg = InheritedAttributes.unconstrained().excluding(List.of(POD));
        InheritedAttributes demandForSelector = demandForAvg.excluding(List.of(REGION));

        // ascend: the leaf reflects its demand, each aggregate folds its own grouping
        SynthesizedAttributes leaf = demandForSelector.reflect();
        SynthesizedAttributes afterAvgWithout = SynthesizedAttributes.foldExcluding(List.of(REGION), leaf);
        SynthesizedAttributes afterWithout = SynthesizedAttributes.foldExcluding(List.of(POD), afterAvgWithout);

        // innermost physical aggregate groups by the full label set (the `_timeseries` identity) and drops every
        // excluded dimension at once
        ResolvedAttributes innermost = afterAvgWithout.translateLeaf(demandForSelector.pathExclusions());
        assertThat(names(innermost.groupings()), equalTo(Set.of("_timeseries")));
        assertThat(names(innermost.excludedDimensions()), equalTo(Set.of("pod", "region")));

        // the outermost aggregate still groups by the full label set (no concrete keys), i.e. `_timeseries`
        ResolvedAttributes outermost = afterWithout.translate(afterAvgWithout.declared());
        assertThat(names(outermost.groupings()), equalTo(Set.of("_timeseries")));
        assertThat(names(outermost.absent()), empty());
    }

    public void testSumByOverAvgWithout() {
        // sum by(cluster,region) ( avg without(region) ( cpu_util ) ) -- region is null-filled at the top

        // descend: each aggregate narrows the demand handed to its child
        InheritedAttributes demandForAvg = InheritedAttributes.unconstrained().limitedTo(List.of(CLUSTER, REGION));
        InheritedAttributes demandForSelector = demandForAvg.excluding(List.of(REGION));

        // ascend: the leaf reflects its demand, each aggregate folds its own grouping
        SynthesizedAttributes leaf = demandForSelector.reflect();
        SynthesizedAttributes afterWithout = SynthesizedAttributes.foldExcluding(List.of(REGION), leaf);
        SynthesizedAttributes afterBy = SynthesizedAttributes.foldIncluding(List.of(CLUSTER, REGION), afterWithout);

        ResolvedAttributes innermost = afterWithout.translateLeaf(demandForSelector.pathExclusions());
        assertThat(names(innermost.groupings()), equalTo(Set.of("cluster")));
        assertThat(names(innermost.excludedDimensions()), equalTo(Set.of("region")));

        ResolvedAttributes outermost = afterBy.translate(afterWithout.declared());
        assertThat(names(outermost.groupings()), equalTo(Set.of("cluster")));
        assertThat(names(outermost.absent()), equalTo(Set.of("region")));
    }

    /**
     * Exercises the label algebra for a {@code WITHOUT} above a {@code WITHOUT} separated by a {@code BY}. Note this
     * shape is also rejected at runtime by {@code PromqlCommand} validation ("nested WITHOUT over WITHOUT is not
     * supported at this time", which looks through intervening {@code BY}), so it is not an executable query; the case is
     * kept to pin down how the algebra folds {@code BY} between two {@code WITHOUT}s.
     */
    public void testMaxWithoutOverSumByOverAvgWithout() {
        // max without(cluster) ( sum by(cluster,region) ( avg without(region) ( cpu_util ) ) )

        // descend: each aggregate narrows the demand handed to its child
        InheritedAttributes demandForSum = InheritedAttributes.unconstrained().excluding(List.of(CLUSTER));
        InheritedAttributes demandForAvg = demandForSum.limitedTo(List.of(CLUSTER, REGION));
        InheritedAttributes demandForSelector = demandForAvg.excluding(List.of(REGION));

        // ascend: the leaf reflects its demand, each aggregate folds its own grouping
        SynthesizedAttributes leaf = demandForSelector.reflect();
        SynthesizedAttributes afterAvgWithout = SynthesizedAttributes.foldExcluding(List.of(REGION), leaf);
        SynthesizedAttributes afterBy = SynthesizedAttributes.foldIncluding(List.of(CLUSTER, REGION), afterAvgWithout);
        SynthesizedAttributes afterWithout = SynthesizedAttributes.foldExcluding(List.of(CLUSTER), afterBy);

        // The intervening BY(cluster,region) clears the inherited exclusions, so only the WITHOUT(region) below the BY
        // reaches the leaf; the outer WITHOUT(cluster) applies over the BY's concrete keys, not at the leaf. (This exact
        // query - two WITHOUTs with a BY between - is rejected in practice; this only pins the algebra composition.)
        ResolvedAttributes innermost = afterAvgWithout.translateLeaf(demandForSelector.pathExclusions());
        assertThat(names(innermost.groupings()), equalTo(Set.of("cluster")));
        assertThat(names(innermost.excludedDimensions()), equalTo(Set.of("region")));

        ResolvedAttributes outermost = afterWithout.translate(afterBy.declared());
        assertThat(names(outermost.groupings()), empty());
        assertThat(names(outermost.absent()), empty());
    }

    public void testSumWithoutOverAvgBy() {
        // sum without(pod) ( avg by(cluster,pod) ( cpu_util ) )

        // descend: each aggregate narrows the demand handed to its child
        InheritedAttributes demandForAvg = InheritedAttributes.unconstrained().excluding(List.of(POD));
        InheritedAttributes demandForSelector = demandForAvg.limitedTo(List.of(CLUSTER, POD));

        // ascend: the leaf reflects its demand, each aggregate folds its own grouping
        SynthesizedAttributes leaf = demandForSelector.reflect();
        SynthesizedAttributes afterBy = SynthesizedAttributes.foldIncluding(List.of(CLUSTER, POD), leaf);
        SynthesizedAttributes afterWithout = SynthesizedAttributes.foldExcluding(List.of(POD), afterBy);

        // The BY(cluster,pod) clears the inherited exclusions, so the leaf has none: the outer WITHOUT(pod) drops pod
        // from the BY's concrete keys (handled at the outer aggregate), not via a leaf `_timeseries` exclusion.
        ResolvedAttributes innermost = afterBy.translateLeaf(demandForSelector.pathExclusions());
        assertThat(names(innermost.groupings()), equalTo(Set.of("cluster", "pod")));
        assertThat(names(innermost.excludedDimensions()), empty());

        ResolvedAttributes outermost = afterWithout.translate(afterBy.declared());
        assertThat(names(outermost.groupings()), equalTo(Set.of("cluster")));
        assertThat(names(outermost.absent()), empty());
    }

    public void testSumByOverAvgBy() {
        // sum by(cluster) ( avg by(cluster,pod) ( cpu_util ) )

        // descend: each aggregate narrows the demand handed to its child
        InheritedAttributes demandForAvg = InheritedAttributes.unconstrained().limitedTo(List.of(CLUSTER));
        InheritedAttributes demandForSelector = demandForAvg.limitedTo(List.of(CLUSTER, POD));

        // ascend: the leaf reflects its demand, each aggregate folds its own grouping
        SynthesizedAttributes leaf = demandForSelector.reflect();
        SynthesizedAttributes afterAvgBy = SynthesizedAttributes.foldIncluding(List.of(CLUSTER, POD), leaf);
        SynthesizedAttributes afterBy = SynthesizedAttributes.foldIncluding(List.of(CLUSTER), afterAvgBy);

        ResolvedAttributes innermost = afterAvgBy.translateLeaf(demandForSelector.pathExclusions());
        assertThat(names(innermost.groupings()), equalTo(Set.of("cluster", "pod")));
        assertThat(names(innermost.excludedDimensions()), empty());

        ResolvedAttributes outermost = afterBy.translate(afterAvgBy.declared());
        assertThat(names(outermost.groupings()), equalTo(Set.of("cluster")));
        assertThat(names(outermost.absent()), empty());
    }

    /**
     * {@link InheritedAttributes#including} models a function-internal requirement such as {@code histogram_quantile}
     * materializing the {@code le} bucket label: it <b>widens</b> the current scope with the extra labels (rather than
     * replacing it like a {@code BY}) and records no exclusion, so the leaf groups by the union as concrete keys.
     */
    public void testIncludingWidensScope() {
        // histogram_quantile over a raw bucket selector: identity {cluster, pod}, then materialize the bucket label.
        Attribute le = attr("le");
        InheritedAttributes demand = InheritedAttributes.unconstrained().limitedTo(List.of(CLUSTER, POD)).including(List.of(le));

        ResolvedAttributes innermost = demand.reflect().translateLeaf(demand.pathExclusions());
        assertThat(names(innermost.groupings()), equalTo(Set.of("cluster", "pod", "le")));
        assertThat(innermost.excludedDimensions(), empty());
    }

    /**
     * Labels are a set keyed by field name, so two distinct {@link Attribute} instances sharing a name must collapse to
     * a single grouping key — never survive as duplicates in {@code grouping}, {@code output}, or the resolved
     * translation. Here a {@code BY} is given the same field name twice with different identities.
     */
    public void testDuplicateFieldNamesCollapseInBy() {
        Attribute clusterA = attr("cluster");
        Attribute clusterB = attr("cluster"); // same field name, different identity

        InheritedAttributes demand = InheritedAttributes.unconstrained().limitedTo(List.of(clusterA, clusterB));
        SynthesizedAttributes leaf = demand.reflect();
        SynthesizedAttributes by = SynthesizedAttributes.foldIncluding(List.of(clusterA, clusterB), leaf);

        // the BY exposes one key per field name, not two
        assertThat(by.declared(), hasSize(1));

        // innermost translation: a single concrete grouping key, nothing missing
        ResolvedAttributes innermost = by.translateLeaf(demand.pathExclusions());
        assertThat(innermost.groupings(), hasSize(1));
        assertThat(names(innermost.groupings()), equalTo(Set.of("cluster")));
        assertThat(innermost.absent(), empty());

        // outer translation against a duplicated child output still resolves to one key
        ResolvedAttributes outer = by.translate(List.of(clusterA, clusterB));
        assertThat(outer.groupings(), hasSize(1));
        assertThat(names(outer.groupings()), equalTo(Set.of("cluster")));
        assertThat(outer.absent(), empty());
    }

    /**
     * A {@code BY} label that the child does not expose is reported as {@code absent} (later null-filled). When the same
     * absent field name is requested twice under different identities it must appear at most once.
     */
    public void testDuplicateAbsentLabelsCollapse() {
        Attribute regionA = attr("region");
        Attribute regionB = attr("region"); // same field name, different identity

        InheritedAttributes demand = InheritedAttributes.unconstrained().limitedTo(List.of(regionA, regionB));
        SynthesizedAttributes by = SynthesizedAttributes.foldIncluding(List.of(regionA, regionB), demand.reflect());

        // child exposes only cluster, so region is absent exactly once
        ResolvedAttributes translation = by.translate(List.of(CLUSTER));
        assertThat(translation.groupings(), empty());
        assertThat(translation.absent(), hasSize(1));
        assertThat(names(translation.absent()), equalTo(Set.of("region")));
    }
}
