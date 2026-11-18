/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.Node.NodeStringFormat;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.into.IntoStats;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Terminal command that writes the query's rows into {@link #destination()}.
 * It is a {@link PipelineBreaker} so the mapper ends the data-node fragment at the write and
 * wraps it in a coordinator-side MERGE_STATS node. {@link #output()} is the one-row {@code [total, created]}
 * result returned to the caller.
 */
public class Into extends UnaryPlan
    implements
        TelemetryAware,
        PipelineBreaker,
        PostAnalysisPlanVerificationAware,
        PostAnalysisVerificationAware {
    public static final String UNSUPPORTED_SOURCE = "INTO currently only supports ROW or local FROM as a source, without FORK";

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Into", Into::new);

    private static final Set<String> LOCAL_ONLY = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);

    private final String destination;
    private final List<Attribute> output;

    public Into(Source source, LogicalPlan child, String destination) {
        this(source, child, destination, IntoStats.createAttributes(source));
    }

    public Into(Source source, LogicalPlan child, String destination, List<Attribute> output) {
        super(source, child);
        this.destination = Objects.requireNonNull(destination);
        this.output = output;
    }

    /**
     * Reads the output too, rather than minting new {@code [total, created]} attributes: the coordinator plan built around this
     * node refers to them by id.
     */
    private Into(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readString(),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeString(destination);
        out.writeNamedWriteableCollection(output);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public String destination() {
        return destination;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public Into replaceChild(LogicalPlan newChild) {
        return new Into(source(), newChild, destination, output);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Into::new, child(), destination, output);
    }

    @Override
    protected AttributeSet computeReferences() {
        return child().resolved() ? child().outputSet() : AttributeSet.EMPTY;
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        // A child feeds its parent, so the root is the last command. The verifier runs this check on every
        // node, and true means that node has this INTO as its input, so a command runs after it.
        // TODO: Support EXPLAIN after INTO for debugging
        return (p, failures) -> {
            if (p.children().contains(this)) {
                failures.add(fail(this, "INTO must be the last command of the query"));
            }
        };
    }

    /**
     * A {@link LocalRelation} is a ROW producer that analysis already materialized, for example {@code DESCRIBE}.
     * Pass {@code false} while the original producer is still in the plan.
     */
    public static boolean unsupportedSource(LogicalPlan child, boolean allowMaterializedRow) {
        return child.anyMatch(p -> {
            if (p instanceof MergePlan) {
                return true;
            }
            if (p instanceof LeafPlan == false || isSupportedSource(p)) {
                return false;
            }
            if (allowMaterializedRow && p instanceof LocalRelation) {
                return false;
            }
            return true;
        });
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        // TODO: before INTO leaves snapshot, gate Into on a minimum TransportVersion in Verifier for mixed-version clusters
        // RejectUnsupportedIntoSource already saw the original producer. DESCRIBE has since replaced it with a LocalRelation.
        if (unsupportedSource(child(), true)) {
            failures.add(fail(this, UNSUPPORTED_SOURCE));
        }
    }

    private static boolean isSupportedSource(LogicalPlan leaf) {
        return leaf instanceof Row
            || (leaf instanceof EsRelation relation
                && relation.indexMode().isTsdb() == false
                && LOCAL_ONLY.containsAll(relation.originalIndices().keySet()));
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName()).append('[').append(mapper.index(destination)).append(']');
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), destination, output);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && destination.equals(((Into) obj).destination) && output.equals(((Into) obj).output);
    }
}
