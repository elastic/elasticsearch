/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.Token;

import java.util.Map;

public class AstBuilder extends LogicalPlanBuilder {
    public AstBuilder(Map<Token, TypedParamValue> params) {
        super(params);
    }
}
