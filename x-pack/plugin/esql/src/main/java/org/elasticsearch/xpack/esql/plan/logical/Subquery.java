/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Subquery extends UnaryPlan implements TelemetryAware, SortAgnostic, ClassifiedAs.SubqueryRelation {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Subquery", Subquery::new);

    private static final TransportVersion SUBQUERY_RELATION_METADATA = TransportVersion.fromName("esql_subquery_relation_metadata");

    private final List<Attribute> ownMetadata;

    public Subquery(Source source, LogicalPlan subqueryPlan) {
        this(source, subqueryPlan, List.of());
    }

    public Subquery(Source source, LogicalPlan subqueryPlan, List<Attribute> ownMetadata) {
        super(source, subqueryPlan);
        this.ownMetadata = ownMetadata;
    }

    private Subquery(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.getTransportVersion().supports(SUBQUERY_RELATION_METADATA)
                ? in.readNamedWriteableCollectionAsList(NamedExpression.class).stream().map(Attribute.class::cast).toList()
                : List.of()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        if (out.getTransportVersion().supports(SUBQUERY_RELATION_METADATA)) {
            out.writeNamedWriteableCollection(ownMetadata);
        }
    }

    public List<Attribute> ownMetadata() {
        return ownMetadata;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Subquery> info() {
        return NodeInfo.create(this, Subquery::new, child(), ownMetadata);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Subquery(source(), newChild, ownMetadata);
    }

    @Override
    public List<Attribute> output() {
        if (ownMetadata.isEmpty()) {
            return child().output();
        }
        List<Attribute> out = new ArrayList<>(child().output());
        out.addAll(ownMetadata);
        return out;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), ownMetadata);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Subquery other = (Subquery) obj;
        return Objects.equals(child(), other.child()) && Objects.equals(ownMetadata, other.ownMetadata);
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName()).append("[]");
    }

    public LogicalPlan plan() {
        return child();
    }
}
