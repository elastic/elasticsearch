/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Binding;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.SecuritySingleNodeTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_PASSWORD_HASHED;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;

public class OperatorPrivilegesSingleNodeTests extends SecuritySingleNodeTestCase {

    private static final String OPERATOR_USER_NAME = "test_operator";

    @Override
    protected String configUsers() {
        return super.configUsers()
            + OPERATOR_USER_NAME + ":" + TEST_PASSWORD_HASHED + "\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles()
            + "limited_operator:\n"
            + "  cluster:\n"
            + "    - 'cluster:admin/voting_config/clear_exclusions'\n"
            + "    - 'monitor'\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles()
            + "limited_operator:" + OPERATOR_USER_NAME + "\n";
    }

    @Override
    protected String configOperatorUsers() {
        return super.configOperatorUsers()
            + "operator:\n"
            + "  - usernames: ['" + OPERATOR_USER_NAME + "']\n";
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());
        // Ensure the new settings can be configured
        builder.put("xpack.security.operator_privileges.enabled", "true");
        return builder.build();
    }

    // TODO: Not all plugins are available in internal cluster tests. Hence not all action names can be checked.
    public void testActionsAreEitherOperatorOnlyOrNot() {
        final Injector injector = node().injector();
        final List<Binding<TransportAction>> bindings = injector.findBindingsByType(TypeLiteral.get(TransportAction.class));

        final List<String> allActionNames = new ArrayList<>(bindings.size());
        for (final Binding<TransportAction> binding : bindings) {
            allActionNames.add(binding.getProvider().get().actionName);
        }

        final Set<String> nonOperatorActions = Set.of(NON_OPERATOR_ACTIONS);
        final Set<String> expectedOperatorOnlyActions = Sets.difference(Set.copyOf(allActionNames), nonOperatorActions);
        final Set<String> actualOperatorOnlyActions = new HashSet<>(CompositeOperatorOnly.ActionOperatorOnly.SIMPLE_ACTIONS);
        assertTrue(actualOperatorOnlyActions.containsAll(expectedOperatorOnlyActions));
        assertFalse(actualOperatorOnlyActions.removeAll(nonOperatorActions));
    }

    public void testSuperuserWillFailToCallOperatorOnlyAction() {
        final ClearVotingConfigExclusionsRequest clearVotingConfigExclusionsRequest = new ClearVotingConfigExclusionsRequest();
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            () -> client().execute(ClearVotingConfigExclusionsAction.INSTANCE, clearVotingConfigExclusionsRequest).actionGet());
        assertThat(e.getCause().getMessage(), containsString("Operator privileges are required for action"));
    }

    public void testOperatorUserWillSucceedToCallOperatorOnlyAction() {
        final Client client = client().filterWithHeader(Map.of(
            "Authorization",
            basicAuthHeaderValue(OPERATOR_USER_NAME, new SecureString(TEST_PASSWORD.toCharArray()))));
        final ClearVotingConfigExclusionsRequest clearVotingConfigExclusionsRequest = new ClearVotingConfigExclusionsRequest();
        client.execute(ClearVotingConfigExclusionsAction.INSTANCE, clearVotingConfigExclusionsRequest).actionGet();
    }

    public static final String[] NON_OPERATOR_ACTIONS = new String[] {
        "cluster:admin/component_template/delete",
        "cluster:admin/component_template/get",
        "cluster:admin/component_template/put",
        "cluster:admin/indices/dangling/delete",
        "cluster:admin/indices/dangling/find",
        "cluster:admin/indices/dangling/import",
        "cluster:admin/indices/dangling/list",
        "cluster:admin/ingest/pipeline/delete",
        "cluster:admin/ingest/pipeline/get",
        "cluster:admin/ingest/pipeline/put",
        "cluster:admin/ingest/pipeline/simulate",
        "cluster:admin/nodes/reload_secure_settings",
        "cluster:admin/persistent/completion",
        "cluster:admin/persistent/remove",
        "cluster:admin/persistent/start",
        "cluster:admin/persistent/update_status",
        "cluster:admin/reindex/rethrottle",
        "cluster:admin/repository/_cleanup",
        "cluster:admin/repository/delete",
        "cluster:admin/repository/get",
        "cluster:admin/repository/put",
        "cluster:admin/repository/verify",
        "cluster:admin/reroute",
        "cluster:admin/script/delete",
        "cluster:admin/script/get",
        "cluster:admin/script/put",
        "cluster:admin/script_context/get",
        "cluster:admin/script_language/get",
        "cluster:admin/settings/update",
        "cluster:admin/snapshot/clone",
        "cluster:admin/snapshot/create",
        "cluster:admin/snapshot/delete",
        "cluster:admin/snapshot/get",
        "cluster:admin/snapshot/restore",
        "cluster:admin/snapshot/status",
        "cluster:admin/snapshot/status[nodes]",
        "cluster:admin/tasks/cancel",
        "cluster:admin/xpack/license/basic_status",
        "cluster:admin/xpack/license/feature_usage",
        "cluster:admin/xpack/license/start_basic",
        "cluster:admin/xpack/license/start_trial",
        "cluster:admin/xpack/license/trial_status",
        "cluster:admin/xpack/monitoring/bulk",
        "cluster:admin/xpack/security/api_key/create",
        "cluster:admin/xpack/security/api_key/get",
        "cluster:admin/xpack/security/api_key/grant",
        "cluster:admin/xpack/security/api_key/invalidate",
        "cluster:admin/xpack/security/cache/clear",
        "cluster:admin/xpack/security/delegate_pki",
        "cluster:admin/xpack/security/oidc/authenticate",
        "cluster:admin/xpack/security/oidc/logout",
        "cluster:admin/xpack/security/oidc/prepare",
        "cluster:admin/xpack/security/privilege/builtin/get",
        "cluster:admin/xpack/security/privilege/cache/clear",
        "cluster:admin/xpack/security/privilege/delete",
        "cluster:admin/xpack/security/privilege/get",
        "cluster:admin/xpack/security/privilege/put",
        "cluster:admin/xpack/security/realm/cache/clear",
        "cluster:admin/xpack/security/role/delete",
        "cluster:admin/xpack/security/role/get",
        "cluster:admin/xpack/security/role/put",
        "cluster:admin/xpack/security/role_mapping/delete",
        "cluster:admin/xpack/security/role_mapping/get",
        "cluster:admin/xpack/security/role_mapping/put",
        "cluster:admin/xpack/security/roles/cache/clear",
        "cluster:admin/xpack/security/saml/authenticate",
        "cluster:admin/xpack/security/saml/complete_logout",
        "cluster:admin/xpack/security/saml/invalidate",
        "cluster:admin/xpack/security/saml/logout",
        "cluster:admin/xpack/security/saml/prepare",
        "cluster:admin/xpack/security/token/create",
        "cluster:admin/xpack/security/token/invalidate",
        "cluster:admin/xpack/security/token/refresh",
        "cluster:admin/xpack/security/user/authenticate",
        "cluster:admin/xpack/security/user/change_password",
        "cluster:admin/xpack/security/user/delete",
        "cluster:admin/xpack/security/user/get",
        "cluster:admin/xpack/security/user/has_privileges",
        "cluster:admin/xpack/security/user/list_privileges",
        "cluster:admin/xpack/security/user/put",
        "cluster:admin/xpack/security/user/set_enabled",
        "cluster:monitor/allocation/explain",
        "cluster:monitor/health",
        "cluster:monitor/main",
        "cluster:monitor/nodes/hot_threads",
        "cluster:monitor/nodes/info",
        "cluster:monitor/nodes/stats",
        "cluster:monitor/nodes/usage",
        "cluster:monitor/remote/info",
        "cluster:monitor/state",
        "cluster:monitor/stats",
        "cluster:monitor/task",
        "cluster:monitor/task/get",
        "cluster:monitor/tasks/lists",
        "cluster:monitor/xpack/info",
        "cluster:monitor/xpack/info/data_tiers",
        "cluster:monitor/xpack/info/monitoring",
        "cluster:monitor/xpack/info/security",
        "cluster:monitor/xpack/license/get",
        "cluster:monitor/xpack/security/saml/metadata",
        "cluster:monitor/xpack/ssl/certificates/get",
        "cluster:monitor/xpack/usage",
        "cluster:monitor/xpack/usage/data_tiers",
        "cluster:monitor/xpack/usage/monitoring",
        "cluster:monitor/xpack/usage/security",
        "indices:admin/aliases",
        "indices:admin/aliases/get",
        "indices:admin/analyze",
        "indices:admin/auto_create",
        "indices:admin/block/add",
        "indices:admin/block/add[s]",
        "indices:admin/cache/clear",
        "indices:admin/close",
        "indices:admin/close[s]",
        "indices:admin/create",
        "indices:admin/delete",
        "indices:admin/flush",
        "indices:admin/flush[s]",
        "indices:admin/forcemerge",
        "indices:admin/get",
        "indices:admin/index_template/delete",
        "indices:admin/index_template/get",
        "indices:admin/index_template/put",
        "indices:admin/index_template/simulate",
        "indices:admin/index_template/simulate_index",
        "indices:admin/mapping/auto_put",
        "indices:admin/mapping/put",
        "indices:admin/mappings/fields/get",
        "indices:admin/mappings/fields/get[index]",
        "indices:admin/mappings/get",
        "indices:admin/open",
        "indices:admin/refresh",
        "indices:admin/refresh[s]",
        "indices:admin/reload_analyzers",
        "indices:admin/resize",
        "indices:admin/resolve/index",
        "indices:admin/rollover",
        "indices:admin/seq_no/add_retention_lease",
        "indices:admin/seq_no/global_checkpoint_sync",
        "indices:admin/seq_no/remove_retention_lease",
        "indices:admin/seq_no/renew_retention_lease",
        "indices:admin/settings/update",
        "indices:admin/shards/search_shards",
        "indices:admin/template/delete",
        "indices:admin/template/get",
        "indices:admin/template/put",
        "indices:admin/validate/query",
        "indices:data/read/async_search/delete",
        "indices:data/read/close_point_in_time",
        "indices:data/read/explain",
        "indices:data/read/field_caps",
        "indices:data/read/field_caps[index]",
        "indices:data/read/get",
        "indices:data/read/mget",
        "indices:data/read/mget[shard]",
        "indices:data/read/msearch",
        "indices:data/read/mtv",
        "indices:data/read/mtv[shard]",
        "indices:data/read/open_point_in_time",
        "indices:data/read/scroll",
        "indices:data/read/scroll/clear",
        "indices:data/read/search",
        "indices:data/read/tv",
        "indices:data/write/bulk",
        "indices:data/write/bulk[s]",
        "indices:data/write/delete",
        "indices:data/write/delete/byquery",
        "indices:data/write/index",
        "indices:data/write/reindex",
        "indices:data/write/update",
        "indices:data/write/update/byquery",
        "indices:monitor/recovery",
        "indices:monitor/segments",
        "indices:monitor/settings/get",
        "indices:monitor/shard_stores",
        "indices:monitor/stats",
        "internal:cluster/nodes/indices/shard/store",
        "internal:gateway/local/meta_state",
        "internal:gateway/local/started_shards"
    };
}
