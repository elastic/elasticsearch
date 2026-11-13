/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * {@link ExternalSourceDatasets} is what a receiving node authorizes a plan by, so it has to find every external node
 * wherever a plan can carry one: a bare exec, a relation deep inside a fragment, one per branch of a merged plan.
 */
public class ExternalSourceDatasetsTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    public void testNoExternalNode() {
        PhysicalPlan plan = sink(new LocalSourceExec(SRC, output(), EmptyLocalSupplier.EMPTY));
        ExternalSourceDatasets datasets = ExternalSourceDatasets.of(plan);
        assertThat(datasets.isEmpty(), is(true));
        assertThat(datasets.names(), empty());
        assertThat(datasets.hasUnnamed(), is(false));
        assertThat(ExternalSourceDatasets.hasExternalNode(plan), is(false));
    }

    public void testNullPlan() {
        assertThat(ExternalSourceDatasets.of(null).isEmpty(), is(true));
        assertThat(ExternalSourceDatasets.hasExternalNode(null), is(false));
    }

    public void testBareExec() {
        ExternalSourceDatasets datasets = ExternalSourceDatasets.of(sink(exec("logs")));
        assertThat(datasets.isEmpty(), is(false));
        assertThat(datasets.names(), contains("logs"));
        assertThat(datasets.hasUnnamed(), is(false));
    }

    public void testUnnamedExec() {
        ExternalSourceDatasets datasets = ExternalSourceDatasets.of(sink(exec(null)));
        assertThat(datasets.isEmpty(), is(false));
        assertThat(datasets.names(), empty());
        assertThat(datasets.hasUnnamed(), is(true));
        assertThat(ExternalSourceDatasets.hasExternalNode(sink(exec(null))), is(true));
    }

    public void testRelationNestedInsideFragment() {
        LogicalPlan fragment = new Limit(
            SRC,
            new Literal(SRC, 10, DataType.INTEGER),
            new Filter(SRC, relation("deep"), new Literal(SRC, true, DataType.BOOLEAN))
        );
        PhysicalPlan plan = sink(new LimitExec(SRC, new FragmentExec(fragment), new Literal(SRC, 10, DataType.INTEGER), null));
        ExternalSourceDatasets datasets = ExternalSourceDatasets.of(plan);
        assertThat(datasets.names(), contains("deep"));
        assertThat(datasets.hasUnnamed(), is(false));
    }

    public void testUnnamedRelationInsideFragment() {
        ExternalSourceDatasets datasets = ExternalSourceDatasets.of(sink(new FragmentExec(relation(null))));
        assertThat(datasets.names(), empty());
        assertThat(datasets.hasUnnamed(), is(true));
    }

    public void testEveryBranchOfAMergedPlan() {
        PhysicalPlan merged = new MergeExec(
            SRC,
            List.of(new FragmentExec(relation("left")), exec("right"), new LocalSourceExec(SRC, output(), EmptyLocalSupplier.EMPTY)),
            output(),
            MergeExec.Kind.UNION
        );
        ExternalSourceDatasets datasets = ExternalSourceDatasets.of(sink(merged));
        assertThat(datasets.names(), contains("left", "right"));
        assertThat(datasets.hasUnnamed(), is(false));
    }

    public void testNamesAreDeduplicatedAndUnnamedIsTracked() {
        PhysicalPlan merged = new MergeExec(
            SRC,
            List.of(exec("same"), new FragmentExec(relation("same")), exec(null)),
            output(),
            MergeExec.Kind.UNION
        );
        ExternalSourceDatasets datasets = ExternalSourceDatasets.of(sink(merged));
        assertThat(datasets.names(), contains("same"));
        assertThat(datasets.hasUnnamed(), is(true));
    }

    private static PhysicalPlan sink(PhysicalPlan child) {
        return new ExchangeSinkExec(SRC, output(), false, child);
    }

    private static List<Attribute> output() {
        return List.of(new FieldAttribute(SRC, "a", new EsField("a", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
    }

    private static ExternalSourceExec exec(String datasetName) {
        return new ExternalSourceExec(SRC, "s3://bucket/data/*.parquet", "parquet", output(), Map.of(), Map.of(), null, 10).withDatasetName(
            datasetName
        );
    }

    private static ExternalRelation relation(String datasetName) {
        List<Attribute> output = output();
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, "parquet", "s3://bucket/data/*.parquet");
        return new ExternalRelation(SRC, "s3://bucket/data/*.parquet", metadata, output, FileList.UNRESOLVED, Map.of(), datasetName);
    }
}
