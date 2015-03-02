/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.Maps;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Before;
import org.junit.Test;

import java.util.Locale;

import static org.hamcrest.Matchers.is;

public class IndexPrivilegeTests extends AbstractPrivilegeTests {

    private String jsonDoc = "{ \"name\" : \"elasticsearch\"}";

    public static final String ROLES =
                    "all_cluster_role:\n" +
                    "  cluster: all\n" +
                    "all_indices_role:\n" +
                    "  indices:\n" +
                    "    '*': all\n" +
                    "all_a_role:\n" +
                    "  indices:\n" +
                    "    'a': all\n" +
                    "read_a_role:\n" +
                    "  indices:\n" +
                    "    'a': read\n" +
                    "write_a_role:\n" +
                    "  indices:\n" +
                    "    'a': write\n" +
                    "read_ab_role:\n" +
                    "  indices:\n" +
                    "    'a': read\n" +
                    "    'b': read\n" +
                    "get_b_role:\n" +
                    "  indices:\n" +
                    "    'b': get\n" +
                    "search_b_role:\n" +
                    "  indices:\n" +
                    "    'b': search\n" +
                    "all_regex_ab_role:\n" +
                    "  indices:\n" +
                    "    '/a|b/': all\n" +
                    "manage_starts_with_a_role:\n" +
                    "  indices:\n" +
                    "    'a*': manage\n" +
                    "data_access_all_role:\n" +
                    "  indices:\n" +
                    "    '*': data_access\n" +
                    "create_c_role:\n" +
                    "  indices:\n" +
                    "    'c': create_index\n" +
                    "monitor_b_role:\n" +
                    "  indices:\n" +
                    "    'b': monitor\n" +
                    "crud_a_role:\n" +
                    "  indices:\n" +
                    "    'a': crud\n" +
                    "delete_b_role:\n" +
                    "  indices:\n" +
                    "    'b': delete\n" +
                    "index_a_role:\n" +
                    "  indices:\n" +
                    "    'a': index\n" +
                    "search_a_role:\n" +
                    "  indices:\n" +
                    "    'a': search\n" +
                    "\n";

    public static final String USERS =
            "admin:{plain}passwd\n" +
            "u1:{plain}passwd\n" +
            "u2:{plain}passwd\n" +
            "u3:{plain}passwd\n" +
            "u4:{plain}passwd\n" +
            "u5:{plain}passwd\n" +
            "u6:{plain}passwd\n" +
            "u7:{plain}passwd\n" +
            "u8:{plain}passwd\n" +
            "u9:{plain}passwd\n" +
            "u10:{plain}passwd\n" +
            "u11:{plain}passwd\n" +
            "u12:{plain}passwd\n" +
            "u13:{plain}passwd\n" +
            "u14:{plain}passwd\n" +
            "u15:{plain}passwd\n";

    public static final String USERS_ROLES =
            "all_indices_role:admin,u8\n" +
            "all_cluster_role:admin\n" +
            "all_a_role:u1,u2,u6\n" +
            "read_a_role:u1,u5,u14\n" +
            "write_a_role:u9\n" +
            "read_ab_role:u2,u4,u9\n" +
            "get_b_role:u3,u5,u8,u10\n" +
            "search_b_role:u6,u10,u13\n" +
            "all_regex_ab_role:u3\n" +
            "manage_starts_with_a_role:u4,u15\n" +
            "data_access_all_role:u12\n" +
            "create_c_role:u11\n" +
            "monitor_b_role:u14\n" +
            "crud_a_role:u12\n" +
            "delete_b_role:u11\n" +
            "index_a_role:u13\n" +
            "search_a_role:u15\n" ;

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

    @Before
    public void insertBaseDocumentsAsAdmin() throws Exception {
        // indices: a,b,c,abc
        ImmutableMap<String, String> params = ImmutableMap.of("refresh", "true");
        assertAccessIsAllowed("admin", "PUT", "/a/foo/1", jsonDoc, params);
        assertAccessIsAllowed("admin", "PUT", "/b/foo/1", jsonDoc, params);
        assertAccessIsAllowed("admin", "PUT", "/c/foo/1", jsonDoc, params);
        assertAccessIsAllowed("admin", "PUT", "/abc/foo/1", jsonDoc, params);
    }

    @Test
    public void testUserU1() throws Exception {
        // u1 has all_a_role and read_a_role
        assertUserIsAllowed("u1", "all", "a");
        assertUserIsDenied("u1", "all", "b");
        assertUserIsDenied("u1", "all", "c");
    }

    @Test
    public void testUserU2() throws Exception {
        // u2 has all_all and read a/b role
        assertUserIsAllowed("u2", "all", "a");
        assertUserIsAllowed("u2", "read", "b");
        assertUserIsDenied("u2", "write", "b");
        assertUserIsDenied("u2", "monitor", "b");
        assertUserIsDenied("u2", "create_index", "b");
        assertUserIsDenied("u2", "all", "c");
    }

    @Test
    public void testUserU3() throws Exception {
        // u3 has get b role, but all access to a* and b* via regex
        assertUserIsAllowed("u3", "all", "a");
        assertUserIsAllowed("u3", "all", "b");
        assertUserIsDenied("u3", "all", "c");
    }

    @Test
    public void testUserU4() throws Exception {
        // u4 has read access to a/b and manage access to a*
        assertUserIsAllowed("u4", "read", "a");
        assertUserIsAllowed("u4", "manage", "a");
        assertUserIsDenied("u4", "index", "a");

        assertUserIsAllowed("u4", "read", "b");
        assertUserIsDenied("u4", "index", "b");
        assertUserIsDenied("u4", "manage", "b");

        assertUserIsDenied("u4", "all", "c");

        assertUserIsAllowed("u4", "create_index", "an_index");
        assertUserIsAllowed("u4", "manage", "an_index");
    }

    @Test
    public void testUserU5() throws Exception {
        // u5 may read a and get b
        assertUserIsAllowed("u5", "read", "a");
        assertUserIsDenied("u5", "manage", "a");
        assertUserIsDenied("u5", "write", "a");

        assertUserIsAllowed("u5", "get", "b");
        assertUserIsDenied("u5", "manage", "b");
        assertUserIsDenied("u5", "write", "b");
        assertAccessIsDenied("u5", "GET", "/b/_search");
    }

    @Test
    public void testUserU6() throws Exception {
        // u6 has all access on a and search access on b
        assertUserIsAllowed("u6", "all", "a");
        assertUserIsAllowed("u6", "search", "b");
        assertUserIsDenied("u6", "manage", "b");
        assertUserIsDenied("u6", "write", "b");
        assertUserIsDenied("u6", "all", "c");
    }

    @Test
    public void testUserU7() throws Exception {
        // no access at all
        assertUserIsDenied("u7", "all", "a");
        assertUserIsDenied("u7", "all", "b");
        assertUserIsDenied("u7", "all", "c");
    }

    @Test
    public void testUserU8() throws Exception {
        // u8 has admin access and get access on b
        assertUserIsAllowed("u8", "all", "a");
        assertUserIsAllowed("u8", "all", "b");
        assertUserIsAllowed("u8", "all", "c");
    }

    @Test
    public void testUserU9() throws Exception {
        // u9 has write access to a and read access to a/b
        assertUserIsAllowed("u9", "crud", "a");
        assertUserIsDenied("u9", "manage", "a");
        assertUserIsAllowed("u9", "read", "b");
        assertUserIsDenied("u9", "manage", "b");
        assertUserIsDenied("u9", "write", "b");
        assertUserIsDenied("u9", "all", "c");
    }

    @Test
    public void testUserU10() throws Exception {
        // u10 has access on get/search on b
        assertUserIsDenied("u10", "all", "a");
        assertUserIsDenied("u10", "manage", "b");
        assertUserIsDenied("u10", "write", "b");
        assertUserIsAllowed("u10", "search", "b");
        assertUserIsDenied("u10", "all", "c");
    }

    @Test
    public void testUserU11() throws Exception {
        // u11 has access to create c and delete b
        assertUserIsDenied("u11", "all", "a");

        assertUserIsDenied("u11", "manage", "b");
        assertUserIsDenied("u11", "index", "b");
        assertUserIsDenied("u11", "search", "b");
        assertUserIsAllowed("u11", "delete", "b");

        assertAccessIsAllowed("admin", "DELETE", "/c");
        assertUserIsAllowed("u11", "create_index", "c");
        assertUserIsDenied("u11", "data_access", "c");
        assertUserIsDenied("u11", "monitor", "c");
    }

    @Test
    public void testUserU12() throws Exception {
        // u12 has data_access to all indices+ crud access to a
        assertUserIsDenied("u12", "manage", "a");
        assertUserIsAllowed("u12", "data_access", "a");
        assertUserIsDenied("u12", "manage", "b");
        assertUserIsAllowed("u12", "data_access", "b");
        assertUserIsDenied("u12", "manage", "c");
        assertUserIsAllowed("u12", "data_access", "c");
    }

    @Test
    public void testUserU13() throws Exception {
        // u13 has search access on b and index access on a
        assertUserIsDenied("u13", "manage", "a");
        assertUserIsAllowed("u13", "index", "a");
        assertUserIsDenied("u13", "delete", "a");
        assertUserIsDenied("u13", "read", "a");

        assertUserIsDenied("u13", "manage", "b");
        assertUserIsDenied("u13", "write", "b");
        assertUserIsAllowed("u13", "search", "b");

        assertUserIsDenied("u13", "all", "c");
    }

    @Test
    public void testUserU14() throws Exception {
        // u14 has access to read a and monitor b
        assertUserIsDenied("u14", "manage", "a");
        assertUserIsDenied("u14", "write", "a");
        assertUserIsAllowed("u14", "read", "a");

        // FIXME, indices:admin/get authorization missing here for _settings call
        assertUserIsAllowed("u14", "monitor", "b");
        assertUserIsDenied("u14", "create_index", "b");
        assertUserIsDenied("u14", "data_access", "b");

        assertUserIsDenied("u14", "all", "c");
    }

    @Test
    public void testUserU15() throws Exception {
        //u15 has access to manage and search a, so that adding warmer templates work
        assertUserIsAllowed("u15", "manage", "a");
        assertUserIsAllowed("u15", "search", "a");
        assertAccessIsAllowed("u15", "PUT", "/a/_warmer/w1", "{ \"query\" : { \"match_all\" : {} } }");
        assertAccessIsAllowed("u15", "DELETE", "/a/_warmer/w1");

        assertUserIsDenied("u15", "all", "b");
        assertUserIsDenied("u15", "all", "c");
    }

    @Test
    public void testThatUnknownUserIsRejectedProperly() throws Exception {
        HttpResponse response = executeRequest("idonotexist", "GET", "/", null, Maps.newHashMap());
        assertThat(response.getStatusCode(), is(401));
    }

    private void assertUserExecutes(String user, String action, String index, boolean userIsAllowed) throws Exception {
        ImmutableMap<String, String> refreshParams = ImmutableMap.of("refresh", "true");

        switch (action) {
            case "all" :
                if (userIsAllowed) {
                    assertUserIsAllowed(user, "manage", index);
                    assertUserIsAllowed(user, "crud", index);
                } else {
                    assertUserIsDenied(user, "manage", index);
                    assertUserIsDenied(user, "crud", index);
                }
                break;

            case "create_index" :
                if (userIsAllowed) {
                    assertAccessIsAllowed(user, "PUT", "/" + index);
                } else {
                    assertAccessIsDenied(user, "PUT", "/" + index);
                }
                break;

            case "manage" :
                if (userIsAllowed) {
                    assertAccessIsAllowed(user, "DELETE", "/" + index);
                    assertUserIsAllowed(user, "create_index", index);
                    // wait until index ready, but as admin
                    client().admin().cluster().prepareHealth(index).setWaitForGreenStatus().get();
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_refresh");
                    ImmutableMap<String, String> analyzeParams = ImmutableMap.of("text", "test");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_analyze", null, analyzeParams);
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_flush");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_optimize");
                    ImmutableMap<String, String> params = ImmutableMap.of("wait_for_completion", "true");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_upgrade", null, params);
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_close");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_open");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_cache/clear");
                    // indexing a document to have the mapping available, and wait for green state to make sure index is created
                    assertAccessIsAllowed("admin", "PUT", "/" + index + "/foo/1", jsonDoc, refreshParams);
                    client().admin().cluster().prepareHealth(index).setWaitForGreenStatus().get();
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_mapping/foo/field/name");
                    // putting warmers only works if the user is allowed to search as well, as the query gets validated, added an own test for this
                    assertAccessIsAllowed("admin", "PUT", "/" + index + "/_warmer/w1", "{ \"query\" : { \"match_all\" : {} } }");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_warmer/w1");
                    assertAccessIsAllowed(user, "DELETE", "/" + index + "/_warmer/w1");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_settings");
                } else {
                    assertAccessIsDenied(user, "DELETE", "/" + index);
                    assertUserIsDenied(user, "create_index", index);
                    assertAccessIsDenied(user, "POST", "/" + index + "/_refresh");
                    ImmutableMap<String, String> analyzeParams = ImmutableMap.of("text", "test");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_analyze", null, analyzeParams);
                    assertAccessIsDenied(user, "POST", "/" + index + "/_flush");
                    assertAccessIsDenied(user, "POST", "/" + index + "/_optimize");
                    ImmutableMap<String, String> params = ImmutableMap.of("wait_for_completion", "true");
                    assertAccessIsDenied(user, "POST", "/" + index + "/_upgrade", null, params);
                    assertAccessIsDenied(user, "POST", "/" + index + "/_close");
                    assertAccessIsDenied(user, "POST", "/" + index + "/_open");
                    assertAccessIsDenied(user, "POST", "/" + index + "/_cache/clear");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_mapping/foo/field/name");
                    assertAccessIsDenied(user, "PUT", "/" + index + "/_warmer/w1", "{ \"query\" : { \"match_all\" : {} } }");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_warmer/w1");
                    assertAccessIsDenied(user, "DELETE", "/" + index + "/_warmer/w1");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_settings");
                }
                break;

            case "monitor" :
                if (userIsAllowed) {
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_stats");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_status");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_segments");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_recovery");
                } else {
                    assertAccessIsDenied(user, "GET", "/" + index + "/_stats");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_status");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_segments");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_recovery");
                }
                break;

            case "data_access" :
                if (userIsAllowed) {
                    assertUserIsAllowed(user, "crud", index);
                } else {
                    assertUserIsDenied(user, "crud", index);
                }
                break;

            case "crud" :
                if (userIsAllowed) {
                    assertUserIsAllowed(user, "read", index);
                    assertUserIsAllowed(user, "index", index);
                } else {
                    assertUserIsDenied(user, "read", index);
                    assertUserIsDenied(user, "index", index);
                }
                break;

            case "read" :
                if (userIsAllowed) {
                    // admin refresh before executing
                    assertAccessIsAllowed("admin", "GET", "/" + index + "/_refresh");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_count");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_search/exists", "{ \"query\" : { \"match_all\" : {} } }");
                    assertAccessIsAllowed("admin", "GET", "/" + index + "/_search");
                    assertAccessIsAllowed("admin", "GET", "/" + index + "/foo/1");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/foo/1/_explain", "{ \"query\" : { \"match_all\" : {} } }");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/foo/1/_termvector");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/foo/_percolate", "{ \"doc\" : { \"foo\" : \"bar\" } }");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_suggest", "{ \"sgs\" : { \"text\" : \"foo\", \"term\" : { \"field\" : \"body\" } } }");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/foo/_mget", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/foo/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");

                    StringBuilder multiPercolate = new StringBuilder("{\"percolate\" : {\"index\" : \"" + index + "\", \"type\" : \"foo\"}}\n");
                    multiPercolate.append("{\"doc\" : {\"message\" : \"some text\"}}\n");
                    multiPercolate.append("{\"percolate\" : {\"index\" : \"" + index + "\", \"type\" : \"foo\", \"id\" : \"1\"}}\n");
                    multiPercolate.append("{}\n");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/foo/_mpercolate", multiPercolate.toString());

                    assertUserIsAllowed(user, "search", index);
                } else {
                    assertAccessIsDenied(user, "GET", "/" + index + "/_count");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_search/exists");
                    assertAccessIsDenied(user, "GET", "/" + index + "/foo/1/_explain", "{ \"query\" : { \"match_all\" : {} } }");
                    assertAccessIsDenied(user, "GET", "/" + index + "/foo/1/_termvector");
                    assertAccessIsDenied(user, "GET", "/" + index + "/foo/_percolate", "{ \"doc\" : { \"foo\" : \"bar\" } }");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_suggest", "{ \"sgs\" : { \"text\" : \"foo\", \"term\" : { \"field\" : \"body\" } } }");
                    assertAccessIsDenied(user, "GET", "/" + index + "/foo/_mget", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
                    assertAccessIsDenied(user, "GET", "/" + index + "/foo/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");

                    StringBuilder multiPercolate = new StringBuilder("{\"percolate\" : {\"index\" : \"" + index + "\", \"type\" : \"foo\"}}\n");
                    multiPercolate.append("{\"doc\" : {\"message\" : \"some text\"}}\n");
                    multiPercolate.append("{\"percolate\" : {\"index\" : \"" + index + "\", \"type\" : \"foo\", \"id\" : \"1\"}}\n");
                    multiPercolate.append("{}\n");
                    assertAccessIsDenied(user, "GET", "/" + index + "/foo/_mpercolate", multiPercolate.toString());

                    assertUserIsDenied(user, "search", index);
                }
                break;

            case "search" :
                if (userIsAllowed) {
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_search");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_suggest", "{ \"my-suggestion\" : { \"text\":\"elasticsearch\", \"term\" : { \"field\" : \"name\" } } }");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/foo/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
                } else {
                    assertAccessIsDenied(user, "GET", "/" + index + "/_search");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_suggest", "{ \"my-suggestion\" : { \"text\":\"elasticsearch\", \"term\" : { \"field\" : \"name\" } } }");
                    assertAccessIsDenied(user, "GET", "/" + index + "/foo/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
                }
                break;

            case "get" :
                if (userIsAllowed) {
                    assertAccessIsAllowed(user, "GET", "/" + index + "/foo/1");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/foo/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
                } else {
                    assertAccessIsDenied(user, "GET", "/" + index + "/foo/1");
                    assertAccessIsDenied(user, "POST", "/" + index + "/foo/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
                }
                break;

            case "index" :
                if (userIsAllowed) {
                    assertAccessIsAllowed(user, "PUT", "/" + index + "/foo/321", "{ \"foo\" : \"bar\" }");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/foo/321/_update", "{ \"doc\" : { \"foo\" : \"baz\" } }");
                } else {
                    assertAccessIsDenied(user, "PUT", "/" + index + "/foo/321", "{ \"foo\" : \"bar\" }");
                    assertAccessIsDenied(user, "POST", "/" + index + "/foo/321/_update", "{ \"doc\" : { \"foo\" : \"baz\" } }");
                }
                break;

            case "delete" :
                String jsonDoc = "{ \"name\" : \"docToDelete\"}";
                assertAccessIsAllowed("admin", "PUT", "/" + index + "/foo/docToDelete", jsonDoc, refreshParams);
                assertAccessIsAllowed("admin", "PUT", "/" + index + "/foo/docToDelete2", jsonDoc, refreshParams);
                if (userIsAllowed) {
                    assertAccessIsAllowed(user, "DELETE", "/" + index + "/foo/docToDelete");
                    assertAccessIsAllowed(user, "DELETE", "/" + index + "/foo/_query", "{ \"query\" : { \"term\" : { \"name\" : \"docToDelete\" } } }");
                } else {
                    assertAccessIsDenied(user, "DELETE", "/" + index + "/foo/docToDelete");
                    assertAccessIsDenied(user, "DELETE", "/" + index + "/foo/_query", "{ \"query\" : { \"term\" : { \"name\" : \"docToDelete\" } } }");
                }
                break;

            case "write" :
                if (userIsAllowed) {
                    assertUserIsAllowed(user, "index", index);
                    assertUserIsAllowed(user, "delete", index);
                    assertAccessIsAllowed(user, "PUT", "/" + index + "/foo/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
                } else {
                    assertUserIsDenied(user, "index", index);
                    assertUserIsDenied(user, "delete", index);
                    assertAccessIsDenied(user, "PUT", "/" + index + "/foo/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
                }
                break;

            default:
                fail(String.format(Locale.ROOT, "Unknown action %s to execute", action));
        }

    }

    private void assertUserIsAllowed(String user, String action, String index) throws Exception {
        assertUserExecutes(user, action, index, true);
    }

    private void assertUserIsDenied(String user, String action, String index) throws Exception {
        assertUserExecutes(user, action, index, false);
    }
}
