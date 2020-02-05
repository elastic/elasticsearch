/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.function;

/**
 * Marker interface indicating that a function accepts one optional argument (typically the last one).
 * This is used by the {@link FunctionRegistry} to perform validation of function declaration.
 */
public interface OptionalArgument {

}
