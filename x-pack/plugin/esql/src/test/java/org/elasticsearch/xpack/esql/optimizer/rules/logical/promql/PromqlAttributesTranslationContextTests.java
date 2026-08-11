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
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.Header;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.NamedColumn;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.TimeSeriesColumn;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class PromqlAttributesTranslationContextTests extends ESTestCase {

    private static final Attribute CLUSTER = attr("cluster");
    private static final Attribute POD = attr("pod");
    private static final Attribute REGION = attr("region");

    public void testTimeSeriesRequirementsAreAdditive() {
        Header requirement = Header.undefined()
            .including(TimeSeriesColumn.of(List.of(REGION)))
            .including(TimeSeriesColumn.of(List.of(REGION)))
            .including(TimeSeriesColumn.of(List.of(POD, REGION)));
        TimeSeriesColumn identity = TimeSeriesColumn.of(List.of());
        TimeSeriesColumn withoutRegion = TimeSeriesColumn.of(List.of(REGION));
        TimeSeriesColumn withoutPodRegion = TimeSeriesColumn.of(List.of(POD, REGION));

        assertTrue(requirement.success(new Header(List.of(identity), List.of(identity, withoutRegion, withoutPodRegion))));
        assertFalse(requirement.success(new Header(List.of(identity), List.of(identity, withoutRegion))));
    }

    public void testRequirementChecksTimeSeriesColumnsButAllowsConcreteIdentity() {
        Header requirement = Header.undefined().including(TimeSeriesColumn.of(List.of(REGION))).including(List.of(CLUSTER));
        TimeSeriesColumn identity = new TimeSeriesColumn(attr(MetadataAttribute.TIMESERIES), List.of());
        TimeSeriesColumn withoutRegion = new TimeSeriesColumn(attr(MetadataAttribute.TIMESERIES), List.of(REGION));

        assertTrue(requirement.success(new Header(List.of(identity), List.of(identity, withoutRegion, new NamedColumn(CLUSTER)))));
        assertFalse(requirement.success(new Header(List.of(identity), List.of(identity, new NamedColumn(CLUSTER)))));
        assertTrue(Header.undefined().success(concreteHeader(CLUSTER)));
    }

    public void testConcreteLabelWideningPreservesTimeSeriesRequirements() {
        Header requirement = Header.undefined().including(TimeSeriesColumn.of(List.of(POD, REGION))).including(List.of(CLUSTER));
        TimeSeriesColumn identity = TimeSeriesColumn.of(List.of());
        TimeSeriesColumn required = TimeSeriesColumn.of(List.of(POD, REGION));

        assertThat(requirement.labels(), equalTo(List.of(CLUSTER)));
        assertTrue(requirement.success(new Header(List.of(identity), List.of(identity, required))));
    }

    public void testIdentityGroupingDefaultsOnlyUndemandedGrouping() {
        Header surface = Header.undefined().including(List.of(CLUSTER)).withIdentityGrouping();

        assertTrue(surface.hasTimeSeriesGrouping());
        assertTrue(Header.undefined().including(TimeSeriesColumn.of(List.of())).success(surface));
        assertThat(surface.labels(), equalTo(List.of(CLUSTER)));

        Header concrete = concreteHeader(CLUSTER);
        assertThat(concrete.withIdentityGrouping(), sameInstance(concrete));
    }

    public void testRequiringPinsLeafIdentityWhenGroupByEmpty() {
        Header leaf = new Header(List.of(TimeSeriesColumn.of(List.of())), List.of(TimeSeriesColumn.of(List.of())));
        Header withoutPod = leaf.groupedWithout(List.of(POD));
        Header demand = Header.undefined().requiring(withoutPod).including(List.of(CLUSTER));

        assertTrue(demand.hasTimeSeriesGrouping());
        assertThat(demand.withIdentityGrouping(), sameInstance(demand));
        assertThat(demand.groupingExpressions(), hasSize(1));
        assertThat(demand.labels(), equalTo(List.of(CLUSTER)));
        demand.transformExpressions((column, grouping) -> {
            if (grouping) {
                assertThat(((TimeSeriesColumn) column).exclusions(), equalTo(List.of(POD)));
            }
            return column;
        });
        // a second requiring with an already-pinned TA group key only carries the wider identity
        Header nested = demand.requiring(withoutPod.groupedWithout(List.of(REGION)));
        assertThat(nested.groupingExpressions(), hasSize(1));
        assertTrue(
            nested.success(
                new Header(
                    List.of(TimeSeriesColumn.of(List.of(POD))),
                    List.of(TimeSeriesColumn.of(List.of(POD)), TimeSeriesColumn.of(List.of(POD, REGION)))
                )
            )
        );
    }

    public void testNestedWithoutSelectsExactCarriedIdentity() {
        Attribute regionIdentity = attr(MetadataAttribute.TIMESERIES);
        Attribute podRegionIdentity = attr(MetadataAttribute.TIMESERIES);

        TimeSeriesColumn region = new TimeSeriesColumn(regionIdentity, List.of(REGION));
        TimeSeriesColumn podRegion = new TimeSeriesColumn(podRegionIdentity, List.of(POD, REGION));
        Header inner = new Header(List.of(region), List.of(region, podRegion));
        Header outer = inner.groupedWithout(List.of(POD));

        assertThat(
            outer.transformExpressions(
                (column, grouping) -> PromqlAttributesTranslationContext.resolveColumn(column, List.of(regionIdentity, podRegionIdentity))
            ).groupingExpressions().stream().map(expression -> expression.toAttribute()).toList(),
            equalTo(List.of(podRegionIdentity))
        );
    }

    public void testByKeepsConcreteKeysAndReportsMissingLabels() {
        Header by = concreteHeader(CLUSTER).groupedBy(List.of(CLUSTER, REGION));

        Header bound = by.transformExpressions(
            (column, grouping) -> PromqlAttributesTranslationContext.resolveColumn(column, List.of(CLUSTER))
        );
        assertThat(
            bound.groupingExpressions().stream().map(expression -> expression.toAttribute()).toList(),
            equalTo(List.of(CLUSTER, REGION))
        );
    }

    public void testDuplicateConcreteLabelsCollapseByName() {
        Attribute duplicate = attr("cluster");
        Header by = concreteHeader(CLUSTER, duplicate).groupedBy(List.of(CLUSTER, duplicate));

        assertThat(by.labels(), hasSize(1));
        assertThat(
            by.transformExpressions(
                (column, grouping) -> PromqlAttributesTranslationContext.resolveColumn(column, List.of(CLUSTER, duplicate))
            ).groupingExpressions(),
            hasSize(1)
        );
    }

    public void testTransformRunsOnceAndPreservesMembership() {
        NamedColumn proxy = new NamedColumn(CLUSTER);
        Header header = new Header(List.of(proxy), List.of(proxy));
        Attribute transformed = attr("transformed");
        int[] calls = new int[1];

        Header result = header.transformExpressions((column, grouping) -> switch (column) {
            case NamedColumn ignored -> {
                calls[0]++;
                assertTrue(grouping);
                yield new NamedColumn(transformed);
            }
            case TimeSeriesColumn ignored -> throw new AssertionError();
        });

        assertThat(calls[0], equalTo(1));
        assertThat(result.groupingExpressions().getFirst(), sameInstance(result.exposedExpressions().getFirst()));
        assertThat(result.groupingExpressions().getFirst(), sameInstance(transformed));
    }

    public void testResolveLinksProxiesToPlanOutput() {
        Attribute identity = attr(MetadataAttribute.TIMESERIES);
        Header bound = new Header(
            List.of(new TimeSeriesColumn(identity, List.of(REGION))),
            List.of(new TimeSeriesColumn(identity, List.of(REGION)), new NamedColumn(CLUSTER))
        ).transformExpressions((column, grouping) -> PromqlAttributesTranslationContext.resolveColumn(column, List.of(identity, CLUSTER)));
        int[] resolvedKinds = new int[2];

        bound.transformExpressions((column, grouping) -> switch (column) {
            case NamedColumn named -> {
                resolvedKinds[0]++;
                assertThat(named.attribute(), sameInstance(CLUSTER));
                yield named;
            }
            case TimeSeriesColumn timeSeries -> {
                resolvedKinds[1]++;
                assertTrue(grouping);
                assertThat(timeSeries.attribute(), sameInstance(identity));
                assertThat(timeSeries.exclusions(), equalTo(List.of(REGION)));
                yield timeSeries;
            }
        });

        assertThat(resolvedKinds, equalTo(new int[] { 1, 1 }));
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }

    private static Header concreteHeader(Attribute... attributes) {
        List<Column> columns = Arrays.stream(attributes).map(NamedColumn::new).map(Column.class::cast).toList();
        return new Header(columns, columns);
    }
}
