/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.Column;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.EphemeralColumn;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.Header;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.HeaderColumn;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.RequireHeader;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class PromqlAttributesTranslationContextTests extends ESTestCase {

    private static final Attribute CLUSTER = attr("cluster");
    private static final Attribute POD = attr("pod");
    private static final Attribute REGION = attr("region");

    public void testTimeSeriesRequirementsAreAdditive() {
        RequireHeader requirement = RequireHeader.copyInput()
            .requiring(EphemeralColumn.of(List.of(REGION)))
            .requiring(EphemeralColumn.of(List.of(REGION)))
            .requiring(EphemeralColumn.of(List.of(POD, REGION)));
        Header header = requirement.header();

        assertThat(header.additionalEphemeralColumns(), hasSize(2));
        assertThat(names(header.additionalEphemeralColumns().get(0).exclusions()), equalTo(Set.of("region")));
        assertThat(names(header.additionalEphemeralColumns().get(1).exclusions()), equalTo(Set.of("pod", "region")));
    }

    public void testRequirementChecksStrictColumnsButAllowsConcreteIdentity() {
        RequireHeader requirement = RequireHeader.copyInput().requiring(EphemeralColumn.of(List.of(REGION))).including(List.of(CLUSTER));
        EphemeralColumn identity = new EphemeralColumn(attr(MetadataAttribute.TIMESERIES), List.of());
        EphemeralColumn withoutRegion = new EphemeralColumn(attr(MetadataAttribute.TIMESERIES), List.of(REGION));

        assertTrue(requirement.check(new Header(List.of(identity), List.of(identity, withoutRegion, new Column(CLUSTER)))));
        assertFalse(requirement.check(new Header(List.of(identity), List.of(identity, new Column(CLUSTER)))));
        assertTrue(RequireHeader.copyInput().check(Header.of(List.of(CLUSTER))));
    }

    public void testConcreteLabelWideningPreservesTimeSeriesRequirements() {
        RequireHeader requirement = RequireHeader.copyInput()
            .requiring(EphemeralColumn.of(List.of(POD, REGION)))
            .including(List.of(CLUSTER));
        Header header = requirement.header();

        assertThat(requirement.demandedLabels(), equalTo(List.of(CLUSTER)));
        assertThat(header.additionalEphemeralColumns().getFirst().exclusions(), equalTo(List.of(POD, REGION)));
    }

    public void testNestedWithoutSelectsExactCarriedIdentity() {
        Attribute regionIdentity = attr(MetadataAttribute.TIMESERIES);
        Attribute podRegionIdentity = attr(MetadataAttribute.TIMESERIES);

        EphemeralColumn region = new EphemeralColumn(regionIdentity, List.of(REGION));
        EphemeralColumn podRegion = new EphemeralColumn(podRegionIdentity, List.of(POD, REGION));
        Header inner = new Header(List.of(region), List.of(region, podRegion));
        Header outer = inner.without(List.of(POD));

        assertThat(names(outer.groupByEphemeral().exclusions()), equalTo(Set.of("region", "pod")));
        assertThat(outer.groupByEphemeral().attribute(), sameInstance(podRegionIdentity));
        assertThat(attributes(outer.bind(List.of(regionIdentity, podRegionIdentity)).groupBy()), equalTo(List.of(podRegionIdentity)));
    }

    public void testByKeepsConcreteKeysAndReportsMissingLabels() {
        Header by = Header.of(List.of(CLUSTER)).groupedBy(List.of(CLUSTER, REGION));

        Header bound = by.bind(List.of(CLUSTER));
        assertThat(attributes(bound.groupBy()), equalTo(List.of(CLUSTER, REGION)));
    }

    public void testDuplicateConcreteLabelsCollapseByName() {
        Attribute duplicate = attr("cluster");
        Header by = Header.of(List.of(CLUSTER, duplicate)).groupedBy(List.of(CLUSTER, duplicate));

        assertThat(by.declared(), hasSize(1));
        assertThat(by.bind(List.of(CLUSTER, duplicate)).groupBy(), hasSize(1));
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }

    private static Set<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).collect(Collectors.toSet());
    }

    private static List<Attribute> attributes(List<HeaderColumn> columns) {
        return columns.stream().map(HeaderColumn::attribute).toList();
    }
}
