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
 * Physical plan for the USER_AGENT command.
 */
public class UserAgentExec extends CompoundOutputEvalExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "UserAgentExec",
        UserAgentExec::new
    );

    private final boolean extractDeviceType;
    private final String regexFile;

    public UserAgentExec(
        Source source,
        PhysicalPlan child,
        Expression input,
        List<String> outputFieldNames,
        List<Attribute> outputFieldAttributes,
        boolean extractDeviceType,
        String regexFile
    ) {
        super(source, child, input, outputFieldNames, outputFieldAttributes);
        this.extractDeviceType = extractDeviceType;
        this.regexFile = regexFile;
    }

    public UserAgentExec(StreamInput in) throws IOException {
        super(in);
        this.extractDeviceType = in.readBoolean();
        this.regexFile = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(extractDeviceType);
        out.writeString(regexFile);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(
            this,
            UserAgentExec::new,
            child(),
            input,
            outputFieldNames(),
            outputFieldAttributes(),
            extractDeviceType,
            regexFile
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
        return new UserAgentExec(source, child, input, outputFieldNames, outputFieldAttributes, this.extractDeviceType, this.regexFile);
    }

    @Override
    protected boolean innerEquals(CompoundOutputEvalExec other) {
        return other instanceof UserAgentExec ua && extractDeviceType == ua.extractDeviceType && Objects.equals(regexFile, ua.regexFile);
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(extractDeviceType, regexFile);
    }

    public boolean extractDeviceType() {
        return extractDeviceType;
    }

    public String regexFile() {
        return regexFile;
    }
}
