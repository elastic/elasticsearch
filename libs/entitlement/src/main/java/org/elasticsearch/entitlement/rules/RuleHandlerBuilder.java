/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules;

import org.elasticsearch.entitlement.rules.function.CheckMethod;
import org.elasticsearch.entitlement.rules.function.VarargCall;
import org.elasticsearch.entitlement.instrumentation.MethodKey;

public class RuleHandlerBuilder<T> {
    private final Class<? extends T> clazz;
    private final MethodKey methodKey;
    private final VarargCall<CheckMethod> checkMethod;

    public RuleHandlerBuilder(Class<? extends T> clazz, MethodKey methodKey, VarargCall<CheckMethod> checkMethod) {
        this.clazz = clazz;
        this.methodKey = methodKey;
        this.checkMethod = checkMethod;
    }

    public ClassMethodBuilder<T> elseThrowNotEntitled() {
        EntitlementRules.registerRule(new EntitlementRule(methodKey, checkMethod));
        return new ClassMethodBuilder<>(clazz);
    }
}
