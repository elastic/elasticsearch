/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.lookup;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public record PainlessInstanceBinding(
    Object targetInstance,
    Method javaMethod,
    Class<?> returnType,
    List<Class<?>> typeParameters,
    Map<Class<?>, Object> annotations
) {

}
