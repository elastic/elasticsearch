/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.injection.spec;

import java.lang.reflect.Parameter;

/**
 * Captures the pertinent info required to inject one of the arguments of a constructor.
 * @param name is for troubleshooting; it's not strictly needed
 * @param formalType is the declared class of the parameter
 * @param injectableType is the target type of the injection dependency
 */
public record ParameterSpec(String name, Class<?> formalType, Class<?> injectableType) {
    public static ParameterSpec from(Parameter parameter) {
        // We currently have no cases where the formal and injectable types are different.
        return new ParameterSpec(parameter.getName(), parameter.getType(), parameter.getType());
    }
}
