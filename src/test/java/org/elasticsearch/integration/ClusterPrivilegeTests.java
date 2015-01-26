/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.Test;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;

@ClusterScope(scope = TEST)
public class ClusterPrivilegeTests extends AbstractPrivilegeTests {

    public static final String ROLES =
                    "role_a:\n" +
                    "  cluster: all\n" +
                    "\n" +
                    "role_b:\n" +
                    "  cluster: monitor\n" +
                    "\n" +
                    "role_c:\n" +
                    "  indices:\n" +
                    "    'someindex': all\n";

    public static final String USERS =
                    "user_a:{plain}passwd\n" +
                    "user_b:{plain}passwd\n" +
                    "user_c:{plain}passwd\n";

    public static final String USERS_ROLES =
                    "role_a:user_a\n" +
                    "role_b:user_b\n" +
                    "role_c:user_c\n";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(InternalNode.HTTP_ENABLED, true)
                .put("action.disable_shutdown", true)
                .build();
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" + ROLES;
    }

    @Override
    protected String configUsers() {
        return super.configUsers() + USERS;
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + USERS_ROLES;
    }

    @Test
    public void testThatClusterPrivilegesWorkAsExpectedViaHttp() throws Exception {
        // user_a can do all the things
        assertAccessIsAllowed("user_a", "GET", "/_cluster/state");
        assertAccessIsAllowed("user_a", "GET", "/_cluster/health");
        assertAccessIsAllowed("user_a", "GET", "/_cluster/settings");
        assertAccessIsAllowed("user_a", "GET", "/_cluster/stats");
        assertAccessIsAllowed("user_a", "GET", "/_cluster/pending_tasks");
        assertAccessIsAllowed("user_a", "GET", "/_nodes/stats");
        assertAccessIsAllowed("user_a", "GET", "/_nodes/hot_threads");
        assertAccessIsAllowed("user_a", "GET", "/_nodes/infos");
        assertAccessIsAllowed("user_a", "POST", "/_cluster/reroute");
        assertAccessIsAllowed("user_a", "PUT", "/_cluster/settings", "{ \"transient\" : { \"indices.ttl.interval\": \"1m\" } }");
        assertAccessIsAllowed("user_a", "POST", "/_cluster/nodes/_all/_shutdown");

        // user_b can do monitoring
        assertAccessIsAllowed("user_b", "GET", "/_cluster/state");
        assertAccessIsAllowed("user_b", "GET", "/_cluster/health");
        assertAccessIsAllowed("user_b", "GET", "/_cluster/settings");
        assertAccessIsAllowed("user_b", "GET", "/_cluster/stats");
        assertAccessIsAllowed("user_b", "GET", "/_cluster/pending_tasks");
        assertAccessIsAllowed("user_b", "GET", "/_nodes/stats");
        assertAccessIsAllowed("user_b", "GET", "/_nodes/hot_threads");
        assertAccessIsAllowed("user_b", "GET", "/_nodes/infos");
        // but no admin stuff
        assertAccessIsDenied("user_b", "POST", "/_cluster/reroute");
        assertAccessIsDenied("user_b", "PUT", "/_cluster/settings", "{ \"transient\" : { \"indices.ttl.interval\": \"1m\" } }");
        assertAccessIsDenied("user_b", "POST", "/_cluster/nodes/_all/_shutdown");

        // sorry user_c, you are not allowed anything
        assertAccessIsDenied("user_c", "GET", "/_cluster/state");
        assertAccessIsDenied("user_c", "GET", "/_cluster/health");
        assertAccessIsDenied("user_c", "GET", "/_cluster/settings");
        assertAccessIsDenied("user_c", "GET", "/_cluster/stats");
        assertAccessIsDenied("user_c", "GET", "/_cluster/pending_tasks");
        assertAccessIsDenied("user_c", "GET", "/_nodes/stats");
        assertAccessIsDenied("user_c", "GET", "/_nodes/hot_threads");
        assertAccessIsDenied("user_c", "GET", "/_nodes/infos");
        assertAccessIsDenied("user_c", "POST", "/_cluster/reroute");
        assertAccessIsDenied("user_c", "PUT", "/_cluster/settings", "{ \"transient\" : { \"indices.ttl.interval\": \"1m\" } }");
        assertAccessIsDenied("user_c", "POST", "/_cluster/nodes/_all/_shutdown");
    }
}
