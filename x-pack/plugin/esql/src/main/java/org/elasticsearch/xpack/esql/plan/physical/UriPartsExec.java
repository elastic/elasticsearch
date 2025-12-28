/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunction;

import java.io.IOException;
import java.util.List;

/**
 * Physical plan for the URI_PARTS command.
 */
public class UriPartsExec extends CompoundOutputEvalExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "UriPartsExec",
        UriPartsExec::new
    );

    public UriPartsExec(
        Source source,
        PhysicalPlan child,
        Expression input,
        List<Attribute> outputFields
    ) {
        super(source, child, input, outputFields, UriPartsFunction.getInstance());
    }

    public UriPartsExec(StreamInput in) throws IOException {
        super(in, UriPartsFunction.getInstance());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public CompoundOutputEvalExec createNewInstance(Source source, PhysicalPlan child, Expression input, List<Attribute> outputFields) {
        return new UriPartsExec(source, child, input, outputFields);
    }

    @Override
    protected boolean configOptionsEqual(CompoundOutputEvalExec other) {
        return other instanceof UriPartsExec;
    }

    @Override
    protected int configOptionsHashCode() {
        return 0;
    }
}
