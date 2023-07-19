/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl;

import org.elasticsearch.ElasticsearchSecurityException;

public final class RoleMapperExpressionValidator {

    public static void validate(RoleMapperExpression expression) {
        if (false == isIncludingRealm(expression)) {
            throw new ElasticsearchSecurityException("role mapping must be scoped to a concrete realm name");
        }
    }

    private static boolean isIncludingRealm(RoleMapperExpression expression) {
        if (expression instanceof AllExpression all) {
            return all.getElements().stream().anyMatch(RoleMapperExpressionValidator::isIncludingRealm);
        } else if (expression instanceof AnyExpression any) {
            return any.getElements().stream().allMatch(RoleMapperExpressionValidator::isIncludingRealm);
        } else if (expression instanceof FieldExpression field) {
            // There must be defined at least one field rule which maps to a concrete realm name.
            // Using wildcards is not allowed as it can be used to define lenient role mappings.
            return field.getField().equals("realm.name") && field.getValues().stream().noneMatch(value -> value.getAutomaton() != null);
        } else if (expression instanceof ExceptExpression except) {
            // We don't care about except. It may exclude some realms (or even all),
            // but it cannot be used to make role mapping lenient.
            return false;
        } else {
            throw new ElasticsearchSecurityException("expression not recognised: " + expression.getClass());
        }
    }

    private RoleMapperExpressionValidator() {
        throw new IllegalAccessError("not allowed!");
    }
}
