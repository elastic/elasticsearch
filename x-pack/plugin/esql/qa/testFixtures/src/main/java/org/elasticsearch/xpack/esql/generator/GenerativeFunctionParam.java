/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

/**
 * A single parameter in an ES|QL function signature, as described by the generated function definition JSON.
 */
public record GenerativeFunctionParam(String name, String type, boolean optional) {}
