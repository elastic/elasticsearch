/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

/**
 * A storage for the argument types and the return type for a specific test case run. See how signatures are collected
 * in {@see AbstractFunctionTestCase#signatures()}.
 *
 * @param argTypes A list of argument types that the function under test accepts.
 * @param returnType The return type of the function under test.
 */
public record TestClassSignatureTypes(List<DataType> argTypes, DataType returnType) {}
