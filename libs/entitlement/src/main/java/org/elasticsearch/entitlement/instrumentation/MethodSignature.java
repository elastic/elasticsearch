/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation;

import java.util.List;

/**
 * A method signature without class information, used for hierarchy-aware rule lookups.
 * Unlike {@link MethodKey}, this does not include the class name, allowing rules defined
 * on a supertype to be inherited by subtypes.
 *
 * @param methodName     the method name
 * @param parameterTypes a list of "internal names" for the parameter types
 */
public record MethodSignature(String methodName, List<String> parameterTypes) {}
