/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Objects;

/**
 * Transient logical plan node produced by the parser for the {@code IP_LOCATION} command.
 * <p>
 * The output columns of {@code IP_LOCATION} depend on the IP database schema, which is only available through the
 * {@link org.elasticsearch.iplocation.api.IpLocationService}. To keep that service out of the parser, the parser emits this
 * unresolved node carrying only the syntax-level options; the {@code ResolveIpLocation} analyzer rule then reads the
 * pre-fetched database metadata from the {@link org.elasticsearch.xpack.esql.analysis.AnalyzerContext} and rewrites it into a
 * resolved {@link IpLocation} node (or leaves it unresolved with a specific message, which the verifier reports).
 * <p>
 * This node is never serialized: it is always resolved (or fails verification) on the coordinator before the plan is mapped to a
 * physical plan. It is therefore intentionally not registered as a {@code NamedWriteable}.
 */
public class UnresolvedIpLocation extends UnaryPlan implements Unresolvable, TelemetryAware {

    private final Expression input;
    private final Attribute outputPrefix;
    private final String databaseFile;
    private final boolean firstOnly;
    @Nullable
    private final List<String> properties;
    private final String unresolvedMsg;

    public UnresolvedIpLocation(
        Source source,
        LogicalPlan child,
        Expression input,
        Attribute outputPrefix,
        String databaseFile,
        boolean firstOnly,
        @Nullable List<String> properties
    ) {
        this(source, child, input, outputPrefix, databaseFile, firstOnly, properties, "Unresolved IP_LOCATION command");
    }

    public UnresolvedIpLocation(
        Source source,
        LogicalPlan child,
        Expression input,
        Attribute outputPrefix,
        String databaseFile,
        boolean firstOnly,
        @Nullable List<String> properties,
        String unresolvedMessage
    ) {
        super(source, child);
        this.input = input;
        this.outputPrefix = outputPrefix;
        this.databaseFile = databaseFile;
        this.firstOnly = firstOnly;
        this.properties = properties;
        this.unresolvedMsg = unresolvedMessage;
    }

    public Expression input() {
        return input;
    }

    public Attribute outputPrefix() {
        return outputPrefix;
    }

    public String databaseFile() {
        return databaseFile;
    }

    public boolean firstOnly() {
        return firstOnly;
    }

    @Nullable
    public List<String> properties() {
        return properties;
    }

    /**
     * Returns a copy of this node carrying a specific failure message, used by the resolution rule when the database or a
     * requested property cannot be resolved. The returned node stays unresolved so the verifier reports the message.
     */
    public UnresolvedIpLocation withUnresolvedMessage(String message) {
        return new UnresolvedIpLocation(source(), child(), input, outputPrefix, databaseFile, firstOnly, properties, message);
    }

    /**
     * Only the input expression needs field resolution; the output prefix is a naming hint, not a column reference. Mirroring
     * {@link CompoundOutputEval}, this excludes the prefix so it is neither requested from field-caps nor flagged as an unknown
     * column while the command is still unresolved.
     */
    @Override
    protected AttributeSet computeReferences() {
        return input.references();
    }

    @Override
    public UnresolvedIpLocation replaceChild(LogicalPlan newChild) {
        return new UnresolvedIpLocation(source(), newChild, input, outputPrefix, databaseFile, firstOnly, properties, unresolvedMsg);
    }

    @Override
    protected NodeInfo<UnresolvedIpLocation> info() {
        return NodeInfo.create(
            this,
            UnresolvedIpLocation::new,
            child(),
            input,
            outputPrefix,
            databaseFile,
            firstOnly,
            properties,
            unresolvedMsg
        );
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public boolean expressionsResolved() {
        return false;
    }

    @Override
    public String unresolvedMessage() {
        return unresolvedMsg;
    }

    @Override
    public String telemetryLabel() {
        return "IP_LOCATION";
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), input, outputPrefix, databaseFile, firstOnly, properties, unresolvedMsg);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        UnresolvedIpLocation other = (UnresolvedIpLocation) obj;
        return Objects.equals(child(), other.child())
            && Objects.equals(input, other.input)
            && Objects.equals(outputPrefix, other.outputPrefix)
            && Objects.equals(databaseFile, other.databaseFile)
            && firstOnly == other.firstOnly
            && Objects.equals(properties, other.properties)
            && Objects.equals(unresolvedMsg, other.unresolvedMsg);
    }
}
