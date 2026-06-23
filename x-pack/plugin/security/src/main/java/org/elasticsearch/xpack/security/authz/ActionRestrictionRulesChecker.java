/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.InternalUser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Checks whether an action is restricted by any configured action restriction rule.
 * Rules are dynamic cluster settings that can be updated at runtime via the cluster settings API.
 * <p>
 * The hot path ({@link #check}) performs zero allocation: a single volatile read of the
 * compiled rules map, followed by predicate tests on cached matchers.
 */
public class ActionRestrictionRulesChecker {

    static final Set<String> NON_RESTRICTABLE_ACTIONS = Set.of("cluster:admin/settings/update");
    private static final ActionRestrictionRulesChecker NOOP = new ActionRestrictionRulesChecker();

    private volatile Map<String, ActionRestrictionRules.Rule> rules = Map.of();
    private final Supplier<DiscoveryNode> localNodeSupplier;
    private final ProjectResolver projectResolver;

    private ActionRestrictionRulesChecker() {
        this.localNodeSupplier = () -> null;
        this.projectResolver = null;
    }

    /**
     * Returns a no-op checker that never restricts any action.
     */
    public static ActionRestrictionRulesChecker noop() {
        return NOOP;
    }

    public ActionRestrictionRulesChecker(
        Supplier<DiscoveryNode> localNodeSupplier,
        ProjectResolver projectResolver,
        Settings initialSettings,
        ClusterSettings clusterSettings
    ) {
        this.localNodeSupplier = localNodeSupplier;
        this.projectResolver = projectResolver;
        loadInitialRules(initialSettings);
        clusterSettings.addAffixGroupUpdateConsumer(
            List.of(
                ActionRestrictionRules.ACTIONS,
                ActionRestrictionRules.NODE_IDS,
                ActionRestrictionRules.NODE_ROLES,
                ActionRestrictionRules.PROJECT_IDS,
                ActionRestrictionRules.USERS,
                ActionRestrictionRules.EXEMPT_ROLES
            ),
            this::updateRule
        );
    }

    private void loadInitialRules(Settings settings) {
        Set<String> namespaces = ActionRestrictionRules.ACTIONS.getNamespaces(settings);
        if (namespaces.isEmpty()) {
            return;
        }
        Map<String, ActionRestrictionRules.Rule> initialRules = new HashMap<>();
        for (String namespace : namespaces) {
            initialRules.put(namespace, ActionRestrictionRules.Rule.fromSettings(namespace, settings));
        }
        this.rules = Map.copyOf(initialRules);
    }

    private synchronized void updateRule(String ruleName, Settings ruleSettings) {
        Map<String, ActionRestrictionRules.Rule> newRules = new HashMap<>(this.rules);
        if (ruleSettings.isEmpty()) {
            newRules.remove(ruleName);
        } else {
            newRules.put(ruleName, ActionRestrictionRules.Rule.fromSettings(ruleName, ruleSettings));
        }
        this.rules = Map.copyOf(newRules);
    }

    /**
     * Checks whether the given action is restricted for the given authentication.
     *
     * @return a human-readable restriction message if the action is restricted, or {@code null} if allowed
     */
    @Nullable
    public String check(Authentication authentication, String action) {
        if (action.startsWith("internal:")) {
            return null;
        }
        if (NON_RESTRICTABLE_ACTIONS.contains(action)) {
            return null;
        }
        if (authentication.getEffectiveSubject().getUser() instanceof InternalUser) {
            return null;
        }

        Map<String, ActionRestrictionRules.Rule> currentRules = this.rules;
        if (currentRules.isEmpty()) {
            return null;
        }

        for (var entry : currentRules.entrySet()) {
            ActionRestrictionRules.Rule rule = entry.getValue();
            if (rule.actionMatcher().test(action)
                && matchesNode(rule)
                && matchesProject(rule)
                && matchesUser(rule, authentication)
                && isNotExempt(rule, authentication)) {
                return "action [" + action + "] is restricted by rule [" + entry.getKey() + "]";
            }
        }
        return null;
    }

    private boolean matchesNode(ActionRestrictionRules.Rule rule) {
        if (rule.nodeIds().isEmpty() && rule.nodeRoles().isEmpty()) {
            return true;
        }
        DiscoveryNode localNode = localNodeSupplier.get();
        if (rule.nodeIds().isEmpty() == false && rule.nodeIds().contains(localNode.getId())) {
            return true;
        }
        if (rule.nodeRoles().isEmpty() == false) {
            Set<String> localRoleNames = localNode.getRoles().stream().map(DiscoveryNodeRole::roleName).collect(Collectors.toSet());
            for (String roleFilter : rule.nodeRoles()) {
                if (localRoleNames.contains(roleFilter)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean matchesProject(ActionRestrictionRules.Rule rule) {
        if (rule.projectIds().isEmpty()) {
            return true;
        }
        if (projectResolver == null) {
            return false;
        }
        return rule.projectIds().contains(projectResolver.getProjectId().id());
    }

    private static boolean matchesUser(ActionRestrictionRules.Rule rule, Authentication authentication) {
        if (rule.users().isEmpty()) {
            return true;
        }
        return rule.users().contains(authentication.getEffectiveSubject().getUser().principal());
    }

    private static boolean isNotExempt(ActionRestrictionRules.Rule rule, Authentication authentication) {
        if (rule.exemptRoles().isEmpty()) {
            return true;
        }
        String[] userRoles = authentication.getEffectiveSubject().getUser().roles();
        return Arrays.stream(userRoles).noneMatch(rule.exemptRoles()::contains);
    }
}
