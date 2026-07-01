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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan for the IP_LOCATION command.
 */
public class IpLocationExec extends CompoundOutputEvalExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "IpLocationExec",
        IpLocationExec::new
    );

    private final String databaseFile;
    private final boolean firstOnly;

    public IpLocationExec(
        Source source,
        PhysicalPlan child,
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

    public IpLocationExec(StreamInput in) throws IOException {
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
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(
            this,
            IpLocationExec::new,
            child(),
            input,
            outputFieldNames(),
            outputFieldAttributes(),
            databaseFile,
            firstOnly
        );
    }

    @Override
    public CompoundOutputEvalExec createNewInstance(
        Source source,
        PhysicalPlan child,
        Expression input,
        List<String> outputFieldNames,
        List<Attribute> outputFieldAttributes
    ) {
        return new IpLocationExec(source, child, input, outputFieldNames, outputFieldAttributes, this.databaseFile, this.firstOnly);
    }

    @Override
    protected boolean innerEquals(CompoundOutputEvalExec other) {
        return other instanceof IpLocationExec ip && Objects.equals(databaseFile, ip.databaseFile) && firstOnly == ip.firstOnly;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(databaseFile, firstOnly);
    }

    public String databaseFile() {
        return databaseFile;
    }

    public boolean firstOnly() {
        return firstOnly;
    }
}
