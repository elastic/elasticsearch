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
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.command.UserAgentFunctionBridge;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.SequencedMap;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * The logical plan for the {@code USER_AGENT} command.
 */
public class UserAgent extends CompoundOutputEval<UserAgent> {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "UserAgent",
        UserAgent::new
    );

    private final boolean extractDeviceType;
    private final String regexFile;

    /**
     * Creates the initial logical plan instance, computing output attributes from the filtered output fields.
     * The caller (LogicalPlanBuilder) resolves properties and extractDeviceType into the filtered output fields map.
     */
    public static UserAgent createInitialInstance(
        Source source,
        LogicalPlan child,
        Expression input,
        Attribute outputFieldPrefix,
        boolean extractDeviceType,
        String regexFile,
        SequencedMap<String, Class<?>> filteredOutputFields
    ) {
        List<String> outputFieldNames = filteredOutputFields.keySet().stream().toList();
        List<Attribute> outputFieldAttributes = computeOutputAttributes(filteredOutputFields, outputFieldPrefix.name(), source);
        return new UserAgent(source, child, input, outputFieldNames, outputFieldAttributes, extractDeviceType, regexFile);
    }

    public UserAgent(
        Source source,
        LogicalPlan child,
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

    public UserAgent(StreamInput in) throws IOException {
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
    public UserAgent createNewInstance(
        Source source,
        LogicalPlan child,
        Expression input,
        List<String> outputFieldNames,
        List<Attribute> outputFieldAttributes
    ) {
        return new UserAgent(source, child, input, outputFieldNames, outputFieldAttributes, this.extractDeviceType, this.regexFile);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(
            this,
            UserAgent::new,
            child(),
            input,
            outputFieldNames(),
            generatedAttributes(),
            extractDeviceType,
            regexFile
        );
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(extractDeviceType, regexFile);
    }

    @Override
    protected boolean innerEquals(CompoundOutputEval<?> other) {
        return other instanceof UserAgent ua && extractDeviceType == ua.extractDeviceType && Objects.equals(regexFile, ua.regexFile);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String telemetryLabel() {
        return "USER_AGENT";
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (input.resolved()) {
            DataType type = input.dataType();
            if (DataType.isNull(type) == false && DataType.isString(type) == false) {
                failures.add(fail(input, "Input for USER_AGENT must be of type [string] but is [{}]", type.typeName()));
            }
        }
    }

    public boolean extractDeviceType() {
        return extractDeviceType;
    }

    public String regexFile() {
        return regexFile;
    }

    /**
     * Returns the full set of possible output fields and their types, keyed by field name and sorted alphabetically.
     * Used by DocsV3Support to render the Kibana command definition's output block.
     */
    public static SortedMap<String, DataType> allOutputFieldTypes() {
        SortedMap<String, DataType> result = new TreeMap<>();
        for (var entry : UserAgentFunctionBridge.getAllOutputFields().entrySet()) {
            result.put(entry.getKey(), DataType.fromJavaType(entry.getValue()));
        }
        return result;
    }
}
