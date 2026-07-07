/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A request to resolve — but NOT execute — an index-abstraction (view or dataset) by <b>name</b> on its home cluster, the
 * schema half of the ES|QL federation <b>execution</b> path. It is the sibling of {@link ExecuteAbstractionRequest}: the
 * coordinator sends this first (during resolution) to obtain the abstraction's real output attributes so it can build a
 * {@code Boundary.REMOTE} leaf with an honest {@code output()}, then sends {@link ExecuteAbstractionRequest} (during
 * execution) to run it.
 *
 * <p>The home cluster resolves the name through its OWN kind-blind {@code SchemaService} umbrella — the same resolution the
 * execution leg re-runs — so the schema this returns matches the plan the execution leg later runs by construction, which
 * is what keeps the schema-drift guard ({@code AbstractionComputeHandler}'s {@code validateSchema}) satisfied.
 *
 * <p>Like {@link ExecuteAbstractionRequest} / {@code TransportResolveSchemaAction.Request} it is an
 * {@link IndicesRequest.Replaceable} carrying {@code resolveViews}/{@code resolveDatasets} options so the security
 * action-filter authorizes {@link #abstractionName} before the {@code indices:}-scoped handler runs it. It carries no
 * {@code Configuration} or session id: no exchange is opened, and the home session synthesizes its own configuration from
 * the {@code FROM <name>} it runs.
 */
final class ResolveAbstractionSchemaRequest extends AbstractTransportRequest implements IndicesRequest.Replaceable {

    /**
     * Index options mirroring {@code TransportResolveSchemaAction.Request.SCHEMA_INDICES_OPTIONS}: the abstraction name
     * must resolve through the view/dataset abstractions during security filtering, exactly as the schema half does.
     */
    private static final IndicesOptions ABSTRACTION_INDICES_OPTIONS = IndicesOptions.builder()
        .wildcardOptions(IndicesOptions.WildcardOptions.builder().allowEmptyExpressions(true))
        .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(true).resolveViews(true).build())
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .build();

    private final String abstractionName;

    private transient String[] indices;

    ResolveAbstractionSchemaRequest(String abstractionName) {
        this.abstractionName = abstractionName;
        this.indices = new String[] { abstractionName };
    }

    ResolveAbstractionSchemaRequest(StreamInput in) throws IOException {
        super(in);
        this.abstractionName = in.readString();
        this.indices = new String[] { abstractionName };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(abstractionName);
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return ABSTRACTION_INDICES_OPTIONS;
    }

    @Override
    public boolean allowsRemoteIndices() {
        return true;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        if (parentTaskId.isSet() == false) {
            assert false : "ResolveAbstractionSchemaRequest must have a parent task";
            throw new IllegalStateException("ResolveAbstractionSchemaRequest must have a parent task");
        }
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return ResolveAbstractionSchemaRequest.this.getDescription();
            }
        };
    }

    String abstractionName() {
        return abstractionName;
    }

    @Override
    public String getDescription() {
        return "abstraction=" + abstractionName;
    }

    @Override
    public String toString() {
        return "ResolveAbstractionSchemaRequest{" + getDescription() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResolveAbstractionSchemaRequest request = (ResolveAbstractionSchemaRequest) o;
        return abstractionName.equals(request.abstractionName) && getParentTask().equals(request.getParentTask());
    }

    @Override
    public int hashCode() {
        return Objects.hash(abstractionName);
    }
}
