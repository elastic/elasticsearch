/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

//test is just too slow, please fix it to not be sleep-based
@BadApple(bugUrl = "https://github.com/elastic/x-plugins/issues/1007")
@ESIntegTestCase.ClusterScope(randomDynamicTemplates = false, maxNumDataNodes = 2)
public class IndexPrivilegeTests extends AbstractPrivilegeTestCase {

    private String jsonDoc = "{ \"name\" : \"elasticsearch\", \"body\": \"foo bar\" }";

    public static final String ROLES =
                    "all_cluster_role:\n" +
                    "  cluster: [ all ]\n" +
                    "all_indices_role:\n" +
                    "  indices:\n" +
                    "    - names: '*'\n" +
                    "      privileges: [ all ]\n" +
                    "all_a_role:\n" +
                    "  indices:\n" +
                    "    - names: 'a'\n" +
                    "      privileges: [ all ]\n" +
                    "read_a_role:\n" +
                    "  indices:\n" +
                    "    - names: 'a'\n" +
                    "      privileges: [ read ]\n" +
                    "read_b_role:\n" +
                    "  indices:\n" +
                    "    - names: 'b'\n" +
                    "      privileges: [ read ]\n" +
                    "write_a_role:\n" +
                    "  indices:\n" +
                    "    - names: 'a'\n" +
                    "      privileges: [ write ]\n" +
                    "read_ab_role:\n" +
                    "  indices:\n" +
                    "    - names: [ 'a', 'b' ]\n" +
                    "      privileges: [ read ]\n" +
                    "all_regex_ab_role:\n" +
                    "  indices:\n" +
                    "    - names: '/a|b/'\n" +
                    "      privileges: [ all ]\n" +
                    "manage_starts_with_a_role:\n" +
                    "  indices:\n" +
                    "    - names: 'a*'\n" +
                    "      privileges: [ manage ]\n" +
                    "read_write_all_role:\n" +
                    "  indices:\n" +
                    "    - names: '*'\n" +
                    "      privileges: [ read, write ]\n" +
                    "create_c_role:\n" +
                    "  indices:\n" +
                    "    - names: 'c'\n" +
                    "      privileges: [ create_index ]\n" +
                    "monitor_b_role:\n" +
                    "  indices:\n" +
                    "    - names: 'b'\n" +
                    "      privileges: [ monitor ]\n" +
                    "read_write_a_role:\n" +
                    "  indices:\n" +
                    "    - names: 'a'\n" +
                    "      privileges: [ read, write ]\n" +
                    "delete_b_role:\n" +
                    "  indices:\n" +
                    "    - names: 'b'\n" +
                    "      privileges: [ delete ]\n" +
                    "index_a_role:\n" +
                    "  indices:\n" +
                    "    - names: 'a'\n" +
                    "      privileges: [ index ]\n" +
                    "\n";

    public static final String USERS =
            "admin:" + USERS_PASSWD_HASHED + "\n" +
            "u1:" + USERS_PASSWD_HASHED + "\n" +
            "u2:" + USERS_PASSWD_HASHED + "\n" +
            "u3:" + USERS_PASSWD_HASHED + "\n" +
            "u4:" + USERS_PASSWD_HASHED + "\n" +
            "u5:" + USERS_PASSWD_HASHED + "\n" +
            "u6:" + USERS_PASSWD_HASHED + "\n" +
            "u7:" + USERS_PASSWD_HASHED + "\n"+
            "u8:" + USERS_PASSWD_HASHED + "\n"+
            "u9:" + USERS_PASSWD_HASHED + "\n" +
            "u11:" + USERS_PASSWD_HASHED + "\n" +
            "u12:" + USERS_PASSWD_HASHED + "\n" +
            "u13:" + USERS_PASSWD_HASHED + "\n" +
            "u14:" + USERS_PASSWD_HASHED + "\n";

    public static final String USERS_ROLES =
            "all_indices_role:admin,u8\n" +
            "all_cluster_role:admin\n" +
            "all_a_role:u1,u2,u6\n" +
            "read_a_role:u1,u5,u14\n" +
            "read_b_role:u3,u5,u6,u8,u13\n" +
            "write_a_role:u9\n" +
            "read_ab_role:u2,u4,u9\n" +
            "all_regex_ab_role:u3\n" +
            "manage_starts_with_a_role:u4\n" +
            "read_write_all_role:u12\n" +
            "create_c_role:u11\n" +
            "monitor_b_role:u14\n" +
            "read_write_a_role:u12\n" +
            "delete_b_role:u11\n" +
            "index_a_role:u13\n";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
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

    // we reduce the number of shards and replicas to help speed up this test since that is not the focus of this test
    @Override
    public int maximumNumberOfReplicas() {
        return 1;
    }

    @Override
    public int maximumNumberOfShards() {
        return 2;
    }

    @Before
    public void insertBaseDocumentsAsAdmin() throws Exception {
        // indices: a,b,c,abc
        Map<String, String> params = singletonMap("refresh", "true");
        assertAccessIsAllowed("admin", "PUT", "/a/foo/1", jsonDoc, params);
        assertAccessIsAllowed("admin", "PUT", "/b/foo/1", jsonDoc, params);
        assertAccessIsAllowed("admin", "PUT", "/c/foo/1", jsonDoc, params);
        assertAccessIsAllowed("admin", "PUT", "/abc/foo/1", jsonDoc, params);
    }

    public void testUserU1() throws Exception {
        // u1 has all_a_role and read_a_role
        assertUserIsAllowed("u1", "all", "a");
        assertUserIsDenied("u1", "all", "b");
        assertUserIsDenied("u1", "all", "c");
    }

    public void testUserU2() throws Exception {
        // u2 has all_all and read a/b role
        assertUserIsAllowed("u2", "all", "a");
        assertUserIsAllowed("u2", "read", "b");
        assertUserIsDenied("u2", "write", "b");
        assertUserIsDenied("u2", "monitor", "b");
        assertUserIsDenied("u2", "create_index", "b");
        assertUserIsDenied("u2", "all", "c");
    }

    public void testUserU3() throws Exception {
        // u3 has read b role, but all access to a* and b* via regex
        assertUserIsAllowed("u3", "all", "a");
        assertUserIsAllowed("u3", "all", "b");
        assertUserIsDenied("u3", "all", "c");
    }

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

    public void testUserU5() throws Exception {
        // u5 may read a and read b
        assertUserIsAllowed("u5", "read", "a");
        assertUserIsDenied("u5", "manage", "a");
        assertUserIsDenied("u5", "write", "a");

        assertUserIsAllowed("u5", "read", "b");
        assertUserIsDenied("u5", "manage", "b");
        assertUserIsDenied("u5", "write", "b");
    }

    public void testUserU6() throws Exception {
        // u6 has all access on a and read access on b
        assertUserIsAllowed("u6", "all", "a");
        assertUserIsAllowed("u6", "read", "b");
        assertUserIsDenied("u6", "manage", "b");
        assertUserIsDenied("u6", "write", "b");
        assertUserIsDenied("u6", "all", "c");
    }

    public void testUserU7() throws Exception {
        // no access at all
        assertUserIsDenied("u7", "all", "a");
        assertUserIsDenied("u7", "all", "b");
        assertUserIsDenied("u7", "all", "c");
    }

    public void testUserU8() throws Exception {
        // u8 has admin access and read access on b
        assertUserIsAllowed("u8", "all", "a");
        assertUserIsAllowed("u8", "all", "b");
        assertUserIsAllowed("u8", "all", "c");
    }

    public void testUserU9() throws Exception {
        // u9 has write access to a and read access to a/b
        assertUserIsAllowed("u9", "crud", "a");
        assertUserIsDenied("u9", "manage", "a");
        assertUserIsAllowed("u9", "read", "b");
        assertUserIsDenied("u9", "manage", "b");
        assertUserIsDenied("u9", "write", "b");
        assertUserIsDenied("u9", "all", "c");
    }

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

    public void testUserU12() throws Exception {
        // u12 has data_access to all indices+ crud access to a
        assertUserIsDenied("u12", "manage", "a");
        assertUserIsAllowed("u12", "data_access", "a");
        assertUserIsDenied("u12", "manage", "b");
        assertUserIsAllowed("u12", "data_access", "b");
        assertUserIsDenied("u12", "manage", "c");
        assertUserIsAllowed("u12", "data_access", "c");
    }

    public void testUserU13() throws Exception {
        // u13 has read access on b and index access on a
        assertUserIsDenied("u13", "manage", "a");
        assertUserIsAllowed("u13", "index", "a");
        assertUserIsDenied("u13", "delete", "a");
        assertUserIsDenied("u13", "read", "a");

        assertUserIsDenied("u13", "manage", "b");
        assertUserIsDenied("u13", "write", "b");
        assertUserIsAllowed("u13", "read", "b");

        assertUserIsDenied("u13", "all", "c");
    }

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

    public void testThatUnknownUserIsRejectedProperly() throws Exception {
        HttpResponse response = executeRequest("idonotexist", "GET", "/", null, new HashMap<>());
        assertThat(response.getStatusCode(), is(401));
    }

    private void assertUserExecutes(String user, String action, String index, boolean userIsAllowed) throws Exception {
        Map<String, String> refreshParams = Collections.emptyMap();//singletonMap("refresh", "true");

        switch (action) {
            case "all" :
                if (userIsAllowed) {
                    assertUserIsAllowed(user, "crud", index);
                    assertUserIsAllowed(user, "manage", index);
                } else {
                    assertUserIsDenied(user, "crud", index);
                    assertUserIsDenied(user, "manage", index);
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
                    Map<String, String> analyzeParams = singletonMap("text", "test");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_analyze", null, analyzeParams);
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_flush");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_forcemerge");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_upgrade", null);
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_close");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_open");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_cache/clear");
                    // indexing a document to have the mapping available, and wait for green state to make sure index is created
                    assertAccessIsAllowed("admin", "PUT", "/" + index + "/foo/1", jsonDoc, refreshParams);
                    client().admin().cluster().prepareHealth(index).setWaitForGreenStatus().get();
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_mapping/foo/field/name");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_settings");
                } else {
                    assertAccessIsDenied(user, "DELETE", "/" + index);
                    assertUserIsDenied(user, "create_index", index);
                    assertAccessIsDenied(user, "POST", "/" + index + "/_refresh");
                    Map<String, String> analyzeParams = singletonMap("text", "test");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_analyze", null, analyzeParams);
                    assertAccessIsDenied(user, "POST", "/" + index + "/_flush");
                    assertAccessIsDenied(user, "POST", "/" + index + "/_forcemerge");
                    assertAccessIsDenied(user, "POST", "/" + index + "/_upgrade", null);
                    assertAccessIsDenied(user, "POST", "/" + index + "/_close");
                    assertAccessIsDenied(user, "POST", "/" + index + "/_open");
                    assertAccessIsDenied(user, "POST", "/" + index + "/_cache/clear");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_mapping/foo/field/name");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_settings");
                }
                break;

            case "monitor" :
                if (userIsAllowed) {
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_stats");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_segments");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_recovery");
                } else {
                    assertAccessIsDenied(user, "GET", "/" + index + "/_stats");
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
                    assertAccessIsAllowed("admin", "GET", "/" + index + "/_search");
                    assertAccessIsAllowed("admin", "GET", "/" + index + "/foo/1");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/foo/1/_explain", "{ \"query\" : { \"match_all\" : {} } }");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/foo/1/_termvector");
                    try {
                        assertAccessIsAllowed(user, "GET", "/" + index + "/foo/_percolate", "{ \"doc\" : { \"foo\" : \"bar\" } }");
                    } catch (Throwable e) {
                        assertThat(e.getMessage(), containsString("field [query] does not exist"));
                    }
                    assertAccessIsAllowed(user, "GET",
                            "/" + index + "/_suggest", "{ \"sgs\" : { \"text\" : \"foo\", \"term\" : { \"field\" : \"body\" } } }");
                    assertAccessIsAllowed(user, "GET",
                            "/" + index + "/foo/_mget", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
                    assertAccessIsAllowed(user, "GET",
                            "/" + index + "/foo/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");

                    StringBuilder multiPercolate =
                            new StringBuilder("{\"percolate\" : {\"index\" : \"" + index + "\", \"type\" : \"foo\"}}\n");
                    multiPercolate.append("{\"doc\" : {\"message\" : \"some text\"}}\n");
                    multiPercolate.append("{\"percolate\" : {\"index\" : \"" + index + "\", \"type\" : \"foo\", \"id\" : \"1\"}}\n");
                    multiPercolate.append("{}\n");
                    try {
                        assertAccessIsAllowed(user, "GET", "/" + index + "/foo/_mpercolate", multiPercolate.toString());
                    } catch (Throwable e) {
                        assertThat(e.getMessage(), containsString("field [query] does not exist"));
                    }

                    assertUserIsAllowed(user, "search", index);
                } else {
                    assertAccessIsDenied(user, "GET", "/" + index + "/_count");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_search");
                    assertAccessIsDenied(user, "GET", "/" + index + "/foo/1/_explain", "{ \"query\" : { \"match_all\" : {} } }");
                    assertAccessIsDenied(user, "GET", "/" + index + "/foo/1/_termvector");
                    assertAccessIsDenied(user, "GET", "/" + index + "/foo/_percolate", "{ \"doc\" : { \"foo\" : \"bar\" } }");
                    assertAccessIsDenied(user,
                            "GET", "/" + index + "/_suggest", "{ \"sgs\" : { \"text\" : \"foo\", \"term\" : { \"field\" : \"body\" } } }");
                    assertAccessIsDenied(user,
                            "GET", "/" + index + "/foo/_mget", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
                    assertAccessIsDenied(user,
                            "GET", "/" + index + "/foo/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");

                    StringBuilder multiPercolate =
                            new StringBuilder("{\"percolate\" : {\"index\" : \"" + index + "\", \"type\" : \"foo\"}}\n");
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
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_suggest", "{ \"my-suggestion\" : { \"text\":\"elasticsearch\", " +
                            "\"term\" : { \"field\" : \"name\" } } }");
                    assertAccessIsAllowed(user,
                            "GET", "/" + index + "/foo/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
                } else {
                    assertAccessIsDenied(user, "GET", "/" + index + "/_search");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_suggest", "{ \"my-suggestion\" : { \"text\":\"elasticsearch\", " +
                            "\"term\" : { \"field\" : \"name\" } } }");
                    assertAccessIsDenied(user,
                            "GET", "/" + index + "/foo/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
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
                } else {
                    assertAccessIsDenied(user, "DELETE", "/" + index + "/foo/docToDelete");
                }
                break;

            case "write" :
                if (userIsAllowed) {
                    assertUserIsAllowed(user, "index", index);
                    assertUserIsAllowed(user, "delete", index);
                    assertAccessIsAllowed(user, "PUT",
                            "/" + index + "/foo/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
                } else {
                    assertUserIsDenied(user, "index", index);
                    assertUserIsDenied(user, "delete", index);
                    assertAccessIsDenied(user,
                            "PUT", "/" + index + "/foo/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
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
