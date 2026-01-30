/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules;

import org.elasticsearch.entitlement.bridge.NotEntitledException;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.rules.function.CheckMethod;
import org.elasticsearch.entitlement.rules.function.VarargCall;

import java.util.function.Consumer;
import java.util.function.Function;

public class VoidRuleHandlerBuilder<T> {
    protected final Consumer<EntitlementRule> addRule;
    protected final Class<? extends T> clazz;
    protected final MethodKey methodKey;
    protected final VarargCall<CheckMethod> checkMethod;

    public VoidRuleHandlerBuilder(
        Consumer<EntitlementRule> addRule,
        Class<? extends T> clazz,
        MethodKey methodKey,
        VarargCall<CheckMethod> checkMethod
    ) {
        this.addRule = addRule;
        this.clazz = clazz;
        this.methodKey = methodKey;
        this.checkMethod = checkMethod;
    }

    public ClassMethodBuilder<T> elseThrowNotEntitled() {
        addRule.accept(new EntitlementRule(methodKey, checkMethod, new EntitlementHandler.NotEntitledEntitlementHandler()));
        return new ClassMethodBuilder<>(addRule, clazz);
    }

    public ClassMethodBuilder<T> elseThrow(Function<NotEntitledException, ? extends Exception> exceptionSupplier) {
        addRule.accept(new EntitlementRule(methodKey, checkMethod, new EntitlementHandler.ExceptionEntitlementHandler(exceptionSupplier)));
        return new ClassMethodBuilder<>(addRule, clazz);
    }
}
