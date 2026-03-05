/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.command.RegisteredDomainFunctionBridge;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * The logical plan for the {@code REGISTERED_DOMAIN} command.
 */
public class RegisteredDomain extends CompoundOutputEval<RegisteredDomain> {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "RegisteredDomain",
        RegisteredDomain::new
    );

    /**
     * Use this static factory method for the initial creation of the logical plan, as it computes the output attributes.
     * Subsequent instantiations (such as deserialization, child replacement, etc.) should use the constructors.
     * @param source source of the command
     * @param child child plan
     * @param input input expression to base the computation on
     * @param outputFieldPrefix the prefix to be used for the output field names
     * @return the logical plan
     */
    public static RegisteredDomain createInitialInstance(Source source, LogicalPlan child, Expression input, Attribute outputFieldPrefix) {
        LinkedHashMap<String, Class<?>> functionOutputFields = RegisteredDomainFunctionBridge.getAllOutputFields();
        List<String> outputFileNames = functionOutputFields.keySet().stream().toList();
        List<Attribute> outputFieldAttributes = computeOutputAttributes(functionOutputFields, outputFieldPrefix.name(), source);
        return new RegisteredDomain(source, child, input, outputFileNames, outputFieldAttributes);
    }

    public RegisteredDomain(
        Source source,
        LogicalPlan child,
        Expression input,
        List<String> outputFieldNames,
        List<Attribute> outputFieldAttributes
    ) {
        super(source, child, input, outputFieldNames, outputFieldAttributes);
    }

    public RegisteredDomain(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RegisteredDomain createNewInstance(
        Source source,
        LogicalPlan child,
        Expression input,
        List<String> outputFieldNames,
        List<Attribute> outputFieldAttributes
    ) {
        return new RegisteredDomain(source, child, input, outputFieldNames, outputFieldAttributes);
    }

    @Override
    protected int innerHashCode() {
        return 0;
    }

    @Override
    protected boolean innerEquals(CompoundOutputEval<?> other) {
        return other instanceof RegisteredDomain;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String telemetryLabel() {
        return "REGISTERED_DOMAIN";
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (input.resolved()) {
            DataType type = input.dataType();
            if (DataType.isString(type) == false) {
                failures.add(fail(input, "Input for REGISTERED_DOMAIN must be of type [string] but is [{}]", type.typeName()));
            }
        }
    }
}
