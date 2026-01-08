/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Map;

public class UriPartsExecSerializationTests extends CompoundOutputEvalExecSerializationTests {

    @Override
    protected CompoundOutputEvalExec createInstance(
        Source source,
        PhysicalPlan child,
        Expression input,
        Map<String, DataType> functionOutputFields,
        List<Attribute> outputFields
    ) {
        return new UriPartsExec(source, child, input, functionOutputFields, outputFields);
    }
}
