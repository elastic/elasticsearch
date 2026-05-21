/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.spi.FunctionPlugin;

/**
 * Registers functions that live in the main ESQL module via SPI.
 */
public class BuiltinFunctions implements FunctionPlugin {
    @Override
    public FunctionDefinition[] functions() {
        return new FunctionDefinition[] { Abs.DEFINITION };
    }
}
