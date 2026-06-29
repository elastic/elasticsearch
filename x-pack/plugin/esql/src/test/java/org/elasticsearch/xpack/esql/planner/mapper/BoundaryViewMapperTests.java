/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MaterializedReadSource;
import org.elasticsearch.xpack.esql.plan.logical.RemoteViewSource;
import org.elasticsearch.xpack.esql.plan.physical.MaterializedReadExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.RemoteViewExec;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Pins the physical leg of boundary-aware view lowering: the first-class source nodes the boundary-aware
 * {@code InlineView} rule produces for non-LOCAL views ({@link RemoteViewSource} / {@link MaterializedReadSource}) are
 * lowered by the {@link Mapper} to their physical exec stubs, carrying name, handle/backing ref, and schema through.
 */
public class BoundaryViewMapperTests extends ESTestCase {

    public void testRemoteViewSourceMapsToRemoteViewExec() {
        RemoteViewSource source = new RemoteViewSource(Source.EMPTY, "remote_view", "home_cluster", List.of());

        PhysicalPlan mapped = new Mapper().map(new Versioned<LogicalPlan>(source, TransportVersion.current()));

        assertThat(mapped, instanceOf(RemoteViewExec.class));
        RemoteViewExec exec = (RemoteViewExec) mapped;
        assertThat(exec.viewName(), equalTo("remote_view"));
        assertThat(exec.handle(), equalTo("home_cluster"));
        assertThat(exec.output(), equalTo(source.output()));
    }

    public void testMaterializedReadSourceMapsToMaterializedReadExec() {
        MaterializedReadSource source = new MaterializedReadSource(Source.EMPTY, "mat_view", ".materialized-mat_view", List.of());

        PhysicalPlan mapped = new Mapper().map(new Versioned<LogicalPlan>(source, TransportVersion.current()));

        assertThat(mapped, instanceOf(MaterializedReadExec.class));
        MaterializedReadExec exec = (MaterializedReadExec) mapped;
        assertThat(exec.viewName(), equalTo("mat_view"));
        assertThat(exec.backingIndex(), equalTo(".materialized-mat_view"));
        assertThat(exec.output(), equalTo(source.output()));
    }
}
