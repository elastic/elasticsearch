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
 * A structure to use as a key/lookup for a method target of instrumentation
 *
 * @param className      the "internal name" of the class: includes the package info, but with periods replaced by slashes
 * @param methodName     the method name
 * @param parameterTypes a list of "internal names" for the parameter types
 */
public record MethodKey(String className, String methodName, List<String> parameterTypes) {}
