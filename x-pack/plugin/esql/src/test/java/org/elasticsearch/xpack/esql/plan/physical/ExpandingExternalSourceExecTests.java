/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.ExpandRead;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/** The scan that reads a collection at element grain, beside the {@link ExternalSourceExec} it is made from. */
public class ExpandingExternalSourceExecTests extends ESTestCase {

    private static final FieldAttribute ID = field("id", DataType.LONG);
    private static final FieldAttribute QTY = field("items.qty", DataType.LONG);

    /** A sibling, not a subclass: a rule matching the plain scan does not match it. */
    public void testIsNotAPlainScan() {
        ExpandingExternalSourceExec expanding = expanding();
        assertThat(expanding, not(instanceOf(ExternalSourceExec.class)));
        assertThat(expanding.attributes(), equalTo(List.<Attribute>of(ID)));
        assertThat(expanding.output(), equalTo(List.<Attribute>of(ID, QTY)));
        assertThat(expanding.expandRead(), equalTo(new ExpandRead("items", List.of(new ExpandRead.Leaf("qty", DataType.LONG)))));
        assertNotEquals(scan(), expanding);
    }

    /**
     * An expanding scan replaced the NestedExpandExec above it, so it is never written to the wire, where the
     * expansion, a local hint, would be dropped and the parents returned unexpanded.
     */
    public void testWriteToRefuses() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> expanding().writeTo(new BytesStreamOutput()));
        assertThat(e.getMessage(), containsString("a scan that expands [items] runs where it was planned"));
    }

    /** The withers every scan shares keep what the expanding scan adds. */
    public void testWithersKeepTheExpansion() {
        ExpandingExternalSourceExec expanding = expanding();
        ExpandingExternalSourceExec filtered = expanding.withPushedFilter("opaque").withDatasetName("orders");
        assertThat(filtered.pushedFilter(), equalTo("opaque"));
        assertThat(filtered.datasetName(), equalTo("orders"));
        assertThat(filtered.expanded(), equalTo(expanding.expanded()));
        assertThat(filtered.expansion(), equalTo(expanding.expansion()));
        assertThat(filtered.output(), equalTo(expanding.output()));
    }

    /**
     * The expanded columns are a property of their own, so an attribute rewrite over the plan reaches them; inside an
     * opaque record it would leave the scan's output pointing at the old attribute.
     */
    public void testAttributeRewritesReachTheExpandedColumns() {
        FieldAttribute renamed = field("items.quantity", DataType.LONG);
        PhysicalPlan rewritten = expanding().transformExpressionsOnly(Attribute.class, a -> a.equals(QTY) ? renamed : a);
        ExpandingExternalSourceExec expanding = as(rewritten);
        assertThat(expanding.expanded(), equalTo(List.<Attribute>of(renamed)));
        assertThat(expanding.output(), equalTo(List.<Attribute>of(ID, renamed)));
    }

    /** Plan printing is the scan's, with the expansion and its parent limit. */
    public void testNodeString() {
        String printed = scan().withExpansion(List.of(QTY), new ExpandingExternalSourceExec.Expansion("items", List.of("qty"), 3))
            .nodeString();
        assertThat(printed, containsString("ExpandingExternalSourceExec[orders.parquet][parquet][expand=items,parentLimit=3]"));
        assertThat(expanding().nodeString(), not(containsString("parentLimit")));
    }

    private static ExpandingExternalSourceExec as(PhysicalPlan plan) {
        assertThat(plan, instanceOf(ExpandingExternalSourceExec.class));
        return (ExpandingExternalSourceExec) plan;
    }

    private static ExternalSourceExec scan() {
        return new ExternalSourceExec(Source.EMPTY, "s3://bucket/orders.parquet", "parquet", List.of(ID), Map.of(), Map.of(), null);
    }

    private static ExpandingExternalSourceExec expanding() {
        return scan().withExpansion(
            List.of(QTY),
            new ExpandingExternalSourceExec.Expansion("items", List.of("qty"), FormatReader.NO_LIMIT)
        );
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }
}
