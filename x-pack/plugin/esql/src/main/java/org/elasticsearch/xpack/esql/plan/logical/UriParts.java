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
import org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

public class UriParts extends CompoundOutputEval<UriParts> {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "UriParts", UriParts::new);

    public UriParts(Source source, LogicalPlan child, Expression input, Attribute outputFieldPrefix) {
        super(source, child, input, outputFieldPrefix, UriPartsFunction.getInstance());
    }

    public UriParts(Source source, LogicalPlan child, Expression input, List<Attribute> outputFields) {
        super(source, child, input, outputFields, UriPartsFunction.getInstance());
    }

    public UriParts(StreamInput in) throws IOException {
        super(in, UriPartsFunction.getInstance());
    }

    @Override
    public UriParts createNewInstance(Source source, LogicalPlan child, Expression input, List<Attribute> outputFields) {
        return new UriParts(source, child, input, outputFields);
    }

    @Override
    protected int configOptionsHashCode() {
        return 0;
    }

    @Override
    protected boolean configOptionsEqual(CompoundOutputEval<?> other) {
        return other instanceof UriParts;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String telemetryLabel() {
        return "URI_PARTS";
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (input.resolved()) {
            DataType type = input.dataType();
            if (DataType.isString(type) == false) {
                failures.add(fail(input, "Input for IP_LOOKUP must be of type [IP] or [String] but is [{}]", type.typeName()));
            }
        }
    }
}
