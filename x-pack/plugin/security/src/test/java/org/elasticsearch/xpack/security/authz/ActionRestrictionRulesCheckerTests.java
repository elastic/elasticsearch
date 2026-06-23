/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ActionRestrictionRulesCheckerTests extends ESTestCase {

    private ActionRestrictionRulesChecker buildChecker(Settings settings, DiscoveryNode localNode) {
        return buildChecker(settings, localNode, DefaultProjectResolver.INSTANCE);
    }

    private ActionRestrictionRulesChecker buildChecker(Settings settings, DiscoveryNode localNode, ProjectResolver projectResolver) {
        java.util.List<org.elasticsearch.common.settings.Setting<?>> settingsList = new java.util.ArrayList<>();
        ActionRestrictionRules.addSettings(settingsList);
        ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(settingsList));
        return new ActionRestrictionRulesChecker(() -> localNode, projectResolver, settings, clusterSettings);
    }

    private DiscoveryNode createNode(String nodeId, Set<DiscoveryNodeRole> roles) {
        return DiscoveryNodeUtils.builder(nodeId).roles(roles).build();
    }

    private Authentication authenticationFor(String principal, String... roles) {
        User user = new User(principal, roles);
        return AuthenticationTestHelper.builder().realm().user(user).build(false);
    }

    public void testNoRulesConfiguredAllowsEverything() {
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checker = buildChecker(Settings.EMPTY, node);
        Authentication auth = authenticationFor("test_user", "some_role");

        assertThat(checker.check(auth, "indices:data/write/index"), nullValue());
        assertThat(checker.check(auth, "cluster:admin/settings/update"), nullValue());
    }

    public void testBasicActionRestriction() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.block_writes.actions", "indices:data/write/*")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node);
        Authentication auth = authenticationFor("test_user", "some_role");

        String result = checker.check(auth, "indices:data/write/index");
        assertThat(result, notNullValue());
        assertThat(result, containsString("block_writes"));

        assertThat(checker.check(auth, "indices:data/read/search"), nullValue());
    }

    public void testExactActionMatch() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.block_delete.actions", "indices:admin/delete")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node);
        Authentication auth = authenticationFor("test_user", "some_role");

        assertThat(checker.check(auth, "indices:admin/delete"), notNullValue());
        assertThat(checker.check(auth, "indices:admin/delete/something"), nullValue());
    }

    public void testSuperuserExemptByDefault() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.block_writes.actions", "indices:data/write/*")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node);

        Authentication superuserAuth = authenticationFor("admin_user", "superuser");
        assertThat(checker.check(superuserAuth, "indices:data/write/index"), nullValue());

        Authentication normalAuth = authenticationFor("normal_user", "viewer");
        assertThat(checker.check(normalAuth, "indices:data/write/index"), notNullValue());
    }

    public void testCustomExemptRoles() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.block_writes.actions", "indices:data/write/*")
            .putList("xpack.security.authz.action_restrictions.rules.block_writes.exempt_roles", "admin_role", "ops_role")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node);

        Authentication exemptAuth = authenticationFor("ops_user", "ops_role");
        assertThat(checker.check(exemptAuth, "indices:data/write/index"), nullValue());

        Authentication superuserAuth = authenticationFor("admin", "superuser");
        assertThat(checker.check(superuserAuth, "indices:data/write/index"), notNullValue());

        Authentication normalAuth = authenticationFor("normal_user", "viewer");
        assertThat(checker.check(normalAuth, "indices:data/write/index"), notNullValue());
    }

    public void testNodeIdFiltering() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.node_rule.actions", "indices:data/write/*")
            .putList("xpack.security.authz.action_restrictions.rules.node_rule.node_ids", "node1", "node2")
            .putList("xpack.security.authz.action_restrictions.rules.node_rule.exempt_roles")
            .build();
        Authentication auth = authenticationFor("test_user", "some_role");

        DiscoveryNode matchingNode = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checkerOnMatchingNode = buildChecker(settings, matchingNode);
        assertThat(checkerOnMatchingNode.check(auth, "indices:data/write/index"), notNullValue());

        DiscoveryNode nonMatchingNode = createNode("node3", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checkerOnNonMatchingNode = buildChecker(settings, nonMatchingNode);
        assertThat(checkerOnNonMatchingNode.check(auth, "indices:data/write/index"), nullValue());
    }

    public void testNodeRoleFiltering() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.frozen_rule.actions", "indices:data/write/*")
            .putList("xpack.security.authz.action_restrictions.rules.frozen_rule.node_roles", "data_frozen")
            .putList("xpack.security.authz.action_restrictions.rules.frozen_rule.exempt_roles")
            .build();
        Authentication auth = authenticationFor("test_user", "some_role");

        DiscoveryNode frozenNode = createNode("node1", Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE));
        ActionRestrictionRulesChecker frozenChecker = buildChecker(settings, frozenNode);
        assertThat(frozenChecker.check(auth, "indices:data/write/index"), notNullValue());

        DiscoveryNode hotNode = createNode("node2", Set.of(DiscoveryNodeRole.DATA_HOT_NODE_ROLE));
        ActionRestrictionRulesChecker hotChecker = buildChecker(settings, hotNode);
        assertThat(hotChecker.check(auth, "indices:data/write/index"), nullValue());
    }

    public void testUserFiltering() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.user_rule.actions", "indices:data/read/search")
            .putList("xpack.security.authz.action_restrictions.rules.user_rule.users", "bad_user")
            .putList("xpack.security.authz.action_restrictions.rules.user_rule.exempt_roles")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node);

        Authentication badUser = authenticationFor("bad_user", "some_role");
        assertThat(checker.check(badUser, "indices:data/read/search"), notNullValue());

        Authentication goodUser = authenticationFor("good_user", "some_role");
        assertThat(checker.check(goodUser, "indices:data/read/search"), nullValue());
    }

    public void testInternalActionsNeverRestricted() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.block_all.actions", "*")
            .putList("xpack.security.authz.action_restrictions.rules.block_all.exempt_roles")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node);
        Authentication auth = authenticationFor("test_user", "some_role");

        assertThat(checker.check(auth, "internal:coordination/something"), nullValue());
    }

    public void testClusterSettingsUpdateNeverRestricted() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.block_all.actions", "*")
            .putList("xpack.security.authz.action_restrictions.rules.block_all.exempt_roles")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node);
        Authentication auth = authenticationFor("test_user", "some_role");

        assertThat(checker.check(auth, "cluster:admin/settings/update"), nullValue());
    }

    public void testInternalUserAlwaysExempt() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.block_all.actions", "*")
            .putList("xpack.security.authz.action_restrictions.rules.block_all.exempt_roles")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node);

        Authentication internalAuth = AuthenticationTestHelper.builder().internal(InternalUsers.SYSTEM_USER).build();
        assertThat(checker.check(internalAuth, "indices:data/write/index"), nullValue());
    }

    public void testMultipleRulesFirstMatchWins() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.rule_a.actions", "indices:data/read/*")
            .putList("xpack.security.authz.action_restrictions.rules.rule_a.exempt_roles")
            .putList("xpack.security.authz.action_restrictions.rules.rule_b.actions", "indices:data/write/*")
            .putList("xpack.security.authz.action_restrictions.rules.rule_b.exempt_roles")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node);
        Authentication auth = authenticationFor("test_user", "some_role");

        assertThat(checker.check(auth, "indices:data/read/search"), notNullValue());
        assertThat(checker.check(auth, "indices:data/write/index"), notNullValue());
        assertThat(checker.check(auth, "cluster:admin/something"), nullValue());
    }

    public void testEmptyActionsPatternMatchesNothing() {
        Settings settings = Settings.builder().putList("xpack.security.authz.action_restrictions.rules.empty_rule.actions").build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node);
        Authentication auth = authenticationFor("test_user", "some_role");

        assertThat(checker.check(auth, "indices:data/write/index"), nullValue());
        assertThat(checker.check(auth, "cluster:admin/something"), nullValue());
    }

    public void testUserWithMultipleRolesOneExempt() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.block_writes.actions", "indices:data/write/*")
            .putList("xpack.security.authz.action_restrictions.rules.block_writes.exempt_roles", "admin_role")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node);

        Authentication auth = authenticationFor("test_user", "viewer", "admin_role");
        assertThat(checker.check(auth, "indices:data/write/index"), nullValue());
    }

    public void testProjectIdFilteringMatchingProject() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.project_rule.actions", "indices:data/write/*")
            .putList("xpack.security.authz.action_restrictions.rules.project_rule.project_ids", "project-abc", "project-def")
            .putList("xpack.security.authz.action_restrictions.rules.project_rule.exempt_roles")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));

        ProjectResolver matchingResolver = mockProjectResolver(ProjectId.fromId("project-abc"));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node, matchingResolver);
        Authentication auth = authenticationFor("test_user", "some_role");

        assertThat(checker.check(auth, "indices:data/write/index"), notNullValue());
    }

    public void testProjectIdFilteringNonMatchingProject() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.project_rule.actions", "indices:data/write/*")
            .putList("xpack.security.authz.action_restrictions.rules.project_rule.project_ids", "project-abc", "project-def")
            .putList("xpack.security.authz.action_restrictions.rules.project_rule.exempt_roles")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));

        ProjectResolver nonMatchingResolver = mockProjectResolver(ProjectId.fromId("project-xyz"));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node, nonMatchingResolver);
        Authentication auth = authenticationFor("test_user", "some_role");

        assertThat(checker.check(auth, "indices:data/write/index"), nullValue());
    }

    public void testProjectIdFilteringEmptyMeansAllProjects() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.global_rule.actions", "indices:data/write/*")
            .putList("xpack.security.authz.action_restrictions.rules.global_rule.exempt_roles")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));

        ProjectResolver resolver = mockProjectResolver(ProjectId.fromId("any-project"));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node, resolver);
        Authentication auth = authenticationFor("test_user", "some_role");

        assertThat(checker.check(auth, "indices:data/write/index"), notNullValue());
    }

    public void testProjectIdCombinedWithUserFiltering() {
        Settings settings = Settings.builder()
            .putList("xpack.security.authz.action_restrictions.rules.combo_rule.actions", "indices:data/write/*")
            .putList("xpack.security.authz.action_restrictions.rules.combo_rule.project_ids", "project-abc")
            .putList("xpack.security.authz.action_restrictions.rules.combo_rule.users", "bad_user")
            .putList("xpack.security.authz.action_restrictions.rules.combo_rule.exempt_roles")
            .build();
        DiscoveryNode node = createNode("node1", Set.of(DiscoveryNodeRole.DATA_ROLE));

        ProjectResolver resolver = mockProjectResolver(ProjectId.fromId("project-abc"));
        ActionRestrictionRulesChecker checker = buildChecker(settings, node, resolver);

        Authentication badUser = authenticationFor("bad_user", "some_role");
        assertThat(checker.check(badUser, "indices:data/write/index"), notNullValue());

        Authentication goodUser = authenticationFor("good_user", "some_role");
        assertThat(checker.check(goodUser, "indices:data/write/index"), nullValue());
    }

    private static ProjectResolver mockProjectResolver(ProjectId projectId) {
        return new ProjectResolver() {
            @Override
            public ProjectId getProjectId() {
                return projectId;
            }

            @Override
            public <E extends Exception> void executeOnProject(ProjectId id, org.elasticsearch.core.CheckedRunnable<E> body) throws E {
                body.run();
            }
        };
    }

    public void testAddSettingsRegistersAllAffixSettings() {
        java.util.List<org.elasticsearch.common.settings.Setting<?>> settingsList = new java.util.ArrayList<>();
        ActionRestrictionRules.addSettings(settingsList);
        assertEquals(6, settingsList.size());
    }
}
