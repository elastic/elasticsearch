/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rolemapping;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AllExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AnyExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExceptExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;

public final class RoleMappingValidator {

    public static final Setting<Boolean> STRICT_ROLE_MAPPING_RULES = Setting.boolSetting(
        SecurityField.setting("role_mapping.rules.strict"),
        false,
        Setting.Property.NodeScope
    );

    public static void validateMappingRules(RoleMapperExpression expression, Settings settings) {
        if (STRICT_ROLE_MAPPING_RULES.get(settings)) {
            if (false == isIncludingRealm(expression)) {
                throw new ElasticsearchSecurityException("role mapping must be scoped to a concrete realm name");
            }
        }
    }

    private static boolean isIncludingRealm(RoleMapperExpression expression) {
        if (expression instanceof AllExpression all) {
            return all.getElements().stream().anyMatch(RoleMappingValidator::isIncludingRealm);
        } else if (expression instanceof AnyExpression any) {
            return any.getElements().stream().allMatch(RoleMappingValidator::isIncludingRealm);
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

    private RoleMappingValidator() {
        throw new IllegalAccessError("not allowed!");
    }
}
