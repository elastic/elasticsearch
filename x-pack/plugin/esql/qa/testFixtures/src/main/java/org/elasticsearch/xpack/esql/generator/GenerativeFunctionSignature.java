/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import java.util.List;

/**
 * One overload of an ES|QL function — its parameter list, return type, and whether it is variadic.
 */
public record GenerativeFunctionSignature(List<GenerativeFunctionParam> params, String returnType, boolean variadic) {}
