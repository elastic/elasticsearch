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
 * A structure to use as a representation of the checkXxx method the instrumentation will inject.
 *
 * @param className the "internal name" of the class: includes the package info, but with periods replaced by slashes
 * @param methodName the checker method name
 * @param parameterDescriptors a list of
 *                             <a href="https://docs.oracle.com/javase/specs/jvms/se23/html/jvms-4.html#jvms-4.3">type descriptors</a>)
 *                             for methodName parameters.
 */
public record CheckMethod(String className, String methodName, List<String> parameterDescriptors) {}
