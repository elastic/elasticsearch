/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.RemoteViewSource;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical lowering of a {@code REMOTE} view, mapped from {@link RemoteViewSource}. Represents "run this view's body on
 * its home cluster and stream back its rows". It carries the remote {@code handle} (home cluster) and the resolved
 * output schema.
 * <p>
 * On the coordinator this leaf <em>will be</em> lowered to an exchange-source operator (a later increment wires
 * {@code LocalExecutionPlanner}): the coordinator opens an exchange to the {@link #handle} home cluster, dispatches
 * {@code ExecuteAbstractionRequest} carrying only the view's {@link #viewName} (its identity — never query text or a
 * coordinator-built plan), and polls result pages back through that exchange. The home cluster resolves the name through
 * its own {@code SchemaService} umbrella (handled by {@code AbstractionComputeHandler}), plans and runs the resolved body
 * locally, and sinks the pages into the exchange. This is the execution half of federation, the sibling of
 * {@code resolve_schema}'s schema half — both name-based, both resolved on the remote.
 */
public class RemoteViewExec extends LeafExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "RemoteViewExec",
        RemoteViewExec::new
    );

    private final String viewName;
    private final String handle;
    private final List<Attribute> output;

    public RemoteViewExec(Source source, String viewName, String handle, List<Attribute> output) {
        super(source);
        this.viewName = viewName;
        this.handle = handle;
        this.output = output;
    }

    private RemoteViewExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readString(),
            in.readString(),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    public String viewName() {
        return viewName;
    }

    public String handle() {
        return handle;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected NodeInfo<RemoteViewExec> info() {
        return NodeInfo.create(this, RemoteViewExec::new, viewName, handle, output);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeString(viewName);
        out.writeString(handle);
        out.writeNamedWriteableCollection(output);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(viewName, handle, output);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RemoteViewExec other = (RemoteViewExec) obj;
        return Objects.equals(viewName, other.viewName) && Objects.equals(handle, other.handle) && Objects.equals(output, other.output);
    }
}
