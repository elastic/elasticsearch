/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Dataset;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.MaterializedDatasetExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.RemoteDatasetExec;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Pins the boundary-aware lowering of a first-class {@link Dataset}: a {@link Dataset.Boundary#REMOTE} /
 * {@link Dataset.Boundary#MATERIALIZED} dataset survives to physical mapping (there is no optimizer fold rule for
 * datasets) and the {@link Mapper} lowers it to its physical exec stub, carrying name, handle/backing ref, and schema
 * through. The {@code LOCAL} parity path is covered by {@code FromDatasetIT} (byte-identical to today's external read).
 */
public class BoundaryDatasetMapperTests extends ESTestCase {

    private static List<Attribute> oneColumnSchema() {
        return List.of(new ReferenceAttribute(Source.EMPTY, "v", DataType.LONG));
    }

    public void testRemoteDatasetMapsToRemoteDatasetExec() {
        List<Attribute> schema = oneColumnSchema();
        Dataset dataset = new Dataset(
            Source.EMPTY,
            "remote_dataset",
            new LocalRelation(Source.EMPTY, schema, EmptyLocalSupplier.EMPTY),
            Dataset.Boundary.REMOTE,
            Dataset.LoweringTarget.remote("home_cluster")
        );

        PhysicalPlan mapped = new Mapper().map(new Versioned<LogicalPlan>(dataset, TransportVersion.current()));

        assertThat(mapped, instanceOf(RemoteDatasetExec.class));
        RemoteDatasetExec exec = (RemoteDatasetExec) mapped;
        assertThat(exec.datasetName(), equalTo("remote_dataset"));
        assertThat(exec.handle(), equalTo("home_cluster"));
        assertThat(exec.output(), equalTo(schema));
    }

    public void testMaterializedDatasetMapsToMaterializedDatasetExec() {
        List<Attribute> schema = oneColumnSchema();
        Dataset dataset = new Dataset(
            Source.EMPTY,
            "mat_dataset",
            new LocalRelation(Source.EMPTY, schema, EmptyLocalSupplier.EMPTY),
            Dataset.Boundary.MATERIALIZED,
            Dataset.LoweringTarget.materialized(".materialized-mat_dataset")
        );

        PhysicalPlan mapped = new Mapper().map(new Versioned<LogicalPlan>(dataset, TransportVersion.current()));

        assertThat(mapped, instanceOf(MaterializedDatasetExec.class));
        MaterializedDatasetExec exec = (MaterializedDatasetExec) mapped;
        assertThat(exec.datasetName(), equalTo("mat_dataset"));
        assertThat(exec.backingIndex(), equalTo(".materialized-mat_dataset"));
        assertThat(exec.output(), equalTo(schema));
    }
}
