/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;

/**
 * Defines the affix settings and compiled rule representation for action restriction rules.
 * Each rule is a named group of settings under {@code xpack.security.authz.action_restrictions.rules.<name>.*}.
 */
public final class ActionRestrictionRules {

    private static final String PREFIX = setting("authz.action_restrictions.rules.");

    public static final Setting.AffixSetting<List<String>> ACTIONS = Setting.affixKeySetting(
        PREFIX,
        "actions",
        key -> Setting.stringListSetting(key, Property.Dynamic, Property.NodeScope)
    );

    public static final Setting.AffixSetting<List<String>> NODE_IDS = Setting.affixKeySetting(
        PREFIX,
        "node_ids",
        key -> Setting.stringListSetting(key, Property.Dynamic, Property.NodeScope)
    );

    public static final Setting.AffixSetting<List<String>> NODE_ROLES = Setting.affixKeySetting(
        PREFIX,
        "node_roles",
        key -> Setting.stringListSetting(key, Property.Dynamic, Property.NodeScope)
    );

    public static final Setting.AffixSetting<List<String>> USERS = Setting.affixKeySetting(
        PREFIX,
        "users",
        key -> Setting.stringListSetting(key, Property.Dynamic, Property.NodeScope)
    );

    public static final Setting.AffixSetting<List<String>> PROJECT_IDS = Setting.affixKeySetting(
        PREFIX,
        "project_ids",
        key -> Setting.stringListSetting(key, Property.Dynamic, Property.NodeScope)
    );

    public static final Setting.AffixSetting<List<String>> EXEMPT_ROLES = Setting.affixKeySetting(
        PREFIX,
        "exempt_roles",
        key -> Setting.stringListSetting(key, List.of("superuser"), Property.Dynamic, Property.NodeScope)
    );

    private ActionRestrictionRules() {}

    /**
     * A compiled restriction rule. The {@code actionMatcher} is pre-built from action patterns
     * and cached; it is never reconstructed on the hot path.
     *
     * @param actionMatcher pre-compiled predicate from action patterns (supports wildcards and Lucene regex via {@link StringMatcher})
     * @param nodeIds       node IDs this rule applies to; empty means all nodes
     * @param nodeRoles     node role names this rule applies to; empty means all nodes
     * @param projectIds    project IDs this rule applies to; empty means all projects
     * @param users         user principals this rule applies to; empty means all users
     * @param exemptRoles   role names exempt from this rule
     */
    public record Rule(
        Predicate<String> actionMatcher,
        Set<String> nodeIds,
        Set<String> nodeRoles,
        Set<String> projectIds,
        Set<String> users,
        Set<String> exemptRoles
    ) {
        /**
         * Builds a compiled rule from the raw settings for a single rule namespace.
         */
        public static Rule fromSettings(String namespace, Settings settings) {
            List<String> actionPatterns = ACTIONS.getConcreteSettingForNamespace(namespace).get(settings);
            Predicate<String> actionMatcher = actionPatterns.isEmpty() ? Predicates.never() : StringMatcher.of(actionPatterns);

            Set<String> nodeIds = Set.copyOf(NODE_IDS.getConcreteSettingForNamespace(namespace).get(settings));
            Set<String> nodeRoles = Set.copyOf(NODE_ROLES.getConcreteSettingForNamespace(namespace).get(settings));
            Set<String> projectIds = Set.copyOf(PROJECT_IDS.getConcreteSettingForNamespace(namespace).get(settings));
            Set<String> users = Set.copyOf(USERS.getConcreteSettingForNamespace(namespace).get(settings));
            Set<String> exemptRoles = Set.copyOf(EXEMPT_ROLES.getConcreteSettingForNamespace(namespace).get(settings));

            return new Rule(actionMatcher, nodeIds, nodeRoles, projectIds, users, exemptRoles);
        }
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(ACTIONS);
        settings.add(NODE_IDS);
        settings.add(NODE_ROLES);
        settings.add(PROJECT_IDS);
        settings.add(USERS);
        settings.add(EXEMPT_ROLES);
    }
}
