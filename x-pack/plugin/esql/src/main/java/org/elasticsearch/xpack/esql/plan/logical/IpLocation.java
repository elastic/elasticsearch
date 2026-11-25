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
import org.elasticsearch.iplocation.api.DatabaseProperty;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.SequencedMap;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * The logical plan for the {@code IP_LOCATION} command.
 */
public class IpLocation extends CompoundOutputEval<IpLocation> {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "IpLocation",
        IpLocation::new
    );

    private final String databaseFile;
    private final boolean firstOnly;

    /**
     * Creates the initial logical plan instance, computing output attributes from the filtered output fields.
     * The caller (LogicalPlanBuilder) resolves properties and DB-default fallback before calling.
     */
    public static IpLocation createInitialInstance(
        Source source,
        LogicalPlan child,
        Expression input,
        Attribute outputFieldPrefix,
        String databaseFile,
        boolean firstOnly,
        SequencedMap<String, Class<?>> filteredOutputFields
    ) {
        List<String> outputFieldNames = filteredOutputFields.keySet().stream().toList();
        List<Attribute> outputFieldAttributes = computeOutputAttributes(
            filteredOutputFields,
            outputFieldPrefix.name(),
            source,
            name -> DatabaseProperty.LOCATION.fieldName().equals(name) ? DataType.GEO_POINT : null
        );
        return new IpLocation(source, child, input, outputFieldNames, outputFieldAttributes, databaseFile, firstOnly);
    }

    public IpLocation(
        Source source,
        LogicalPlan child,
        Expression input,
        List<String> outputFieldNames,
        List<Attribute> outputFieldAttributes,
        String databaseFile,
        boolean firstOnly
    ) {
        super(source, child, input, outputFieldNames, outputFieldAttributes);
        this.databaseFile = databaseFile;
        this.firstOnly = firstOnly;
    }

    public IpLocation(StreamInput in) throws IOException {
        super(in);
        this.databaseFile = in.readString();
        this.firstOnly = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(databaseFile);
        out.writeBoolean(firstOnly);
    }

    @Override
    public IpLocation createNewInstance(
        Source source,
        LogicalPlan child,
        Expression input,
        List<String> outputFieldNames,
        List<Attribute> outputFieldAttributes
    ) {
        return new IpLocation(source, child, input, outputFieldNames, outputFieldAttributes, this.databaseFile, this.firstOnly);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, IpLocation::new, child(), input, outputFieldNames(), generatedAttributes(), databaseFile, firstOnly);
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(databaseFile, firstOnly);
    }

    @Override
    protected boolean innerEquals(CompoundOutputEval<?> other) {
        return other instanceof IpLocation ip && Objects.equals(databaseFile, ip.databaseFile) && firstOnly == ip.firstOnly;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String telemetryLabel() {
        return "IP_LOCATION";
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (input.resolved()) {
            DataType type = input.dataType();
            if (DataType.isNull(type) == false && DataType.isString(type) == false && type != DataType.IP) {
                failures.add(fail(input, "Input for IP_LOCATION must be of type [string] or [ip] but is [{}]", type.typeName()));
            }
        }
    }

    public String databaseFile() {
        return databaseFile;
    }

    public boolean firstOnly() {
        return firstOnly;
    }
}
