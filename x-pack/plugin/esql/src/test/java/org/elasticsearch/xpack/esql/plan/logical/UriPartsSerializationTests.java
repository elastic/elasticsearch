/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

public class UriPartsSerializationTests extends CompoundOutputEvalSerializationTests<UriParts> {
    @Override
    protected UriParts createInitialInstance(Source source, LogicalPlan child, Expression input, Attribute outputFieldPrefix) {
        return UriParts.createInitialInstance(source, child, input, Objects.requireNonNull(outputFieldPrefix));
    }
}
