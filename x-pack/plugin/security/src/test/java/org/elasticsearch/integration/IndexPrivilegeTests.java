/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.Before;

import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.is;

public class IndexPrivilegeTests extends AbstractPrivilegeTestCase {

    private String jsonDoc = "{ \"name\" : \"elasticsearch\", \"body\": \"foo bar\" }";

    private static final String ROLES =
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

    private static final String USERS_ROLES =
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
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" + ROLES;
    }

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(Hasher.resolve(
            randomFrom("pbkdf2", "pbkdf2_1000", "bcrypt", "bcrypt9")).hash(new SecureString("passwd".toCharArray())));

        return super.configUsers() +
            "admin:" + usersPasswdHashed + "\n" +
            "u1:" + usersPasswdHashed + "\n" +
            "u2:" + usersPasswdHashed + "\n" +
            "u3:" + usersPasswdHashed + "\n" +
            "u4:" + usersPasswdHashed + "\n" +
            "u5:" + usersPasswdHashed + "\n" +
            "u6:" + usersPasswdHashed + "\n" +
            "u7:" + usersPasswdHashed + "\n" +
            "u8:" + usersPasswdHashed + "\n" +
            "u9:" + usersPasswdHashed + "\n" +
            "u11:" + usersPasswdHashed + "\n" +
            "u12:" + usersPasswdHashed + "\n" +
            "u13:" + usersPasswdHashed + "\n" +
            "u14:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + USERS_ROLES;
    }

    @Before
    public void insertBaseDocumentsAsAdmin() throws Exception {
        // indices: a,b,c,abc
        for (String index : new String[] {"a", "b", "c", "abc"}) {
            Request request = new Request("PUT", "/" + index + "/_doc/1");
            request.setJsonEntity(jsonDoc);
            request.addParameter("refresh", "true");
            assertAccessIsAllowed("admin", request);
        }
    }

    private static String randomIndex() {
        return randomFrom("a", "b", "c", "abc");
    }

    public void testUserU1() throws Exception {
        // u1 has all_a_role and read_a_role
        assertUserIsAllowed("u1", "all", "a");
        assertUserIsDenied("u1", "all", "b");
        assertUserIsDenied("u1", "all", "c");
        assertAccessIsAllowed("u1",
                "GET", "/" + randomIndex() + "/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
        assertAccessIsAllowed("u1", "POST", "/" + randomIndex() + "/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
        assertAccessIsAllowed("u1", "PUT",
                "/" + randomIndex() + "/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsAllowed("u1",
                "GET", "/" + randomIndex() + "/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
    }

    public void testUserU2() throws Exception {
        // u2 has all_all and read a/b role
        assertUserIsAllowed("u2", "all", "a");
        assertUserIsAllowed("u2", "read", "b");
        assertUserIsDenied("u2", "write", "b");
        assertUserIsDenied("u2", "monitor", "b");
        assertUserIsDenied("u2", "create_index", "b");
        assertUserIsDenied("u2", "all", "c");
        assertAccessIsAllowed("u2",
                "GET", "/" + randomIndex() + "/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
        assertAccessIsAllowed("u2", "POST", "/" + randomIndex() + "/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
        assertAccessIsAllowed("u2", "PUT",
                "/" + randomIndex() + "/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsAllowed("u2",
                "GET", "/" + randomIndex() + "/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
    }

    public void testUserU3() throws Exception {
        // u3 has read b role, but all access to a* and b* via regex
        assertUserIsAllowed("u3", "all", "a");
        assertUserIsAllowed("u3", "all", "b");
        assertUserIsDenied("u3", "all", "c");
        assertAccessIsAllowed("u3",
                "GET", "/" + randomIndex() + "/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
        assertAccessIsAllowed("u3", "POST", "/" + randomIndex() + "/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
        assertAccessIsAllowed("u3", "PUT",
                "/" + randomIndex() + "/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsAllowed("u3",
                "GET", "/" + randomIndex() + "/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
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

        assertAccessIsAllowed("u4",
                "GET", "/" + randomIndex() + "/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
        assertAccessIsAllowed("u4", "POST", "/" + randomIndex() + "/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
        assertAccessIsDenied("u4", "PUT",
                "/" + randomIndex() + "/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsAllowed("u4",
                "GET", "/" + randomIndex() + "/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
    }

    public void testUserU5() throws Exception {
        // u5 may read a and read b
        assertUserIsAllowed("u5", "read", "a");
        assertUserIsDenied("u5", "manage", "a");
        assertUserIsDenied("u5", "write", "a");

        assertUserIsAllowed("u5", "read", "b");
        assertUserIsDenied("u5", "manage", "b");
        assertUserIsDenied("u5", "write", "b");

        assertAccessIsAllowed("u5",
                "GET", "/" + randomIndex() + "/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
        assertAccessIsAllowed("u5", "POST", "/" + randomIndex() + "/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
        assertAccessIsDenied("u5", "PUT",
                "/" + randomIndex() + "/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsAllowed("u5",
                "GET", "/" + randomIndex() + "/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
    }

    public void testUserU6() throws Exception {
        // u6 has all access on a and read access on b
        assertUserIsAllowed("u6", "all", "a");
        assertUserIsAllowed("u6", "read", "b");
        assertUserIsDenied("u6", "manage", "b");
        assertUserIsDenied("u6", "write", "b");
        assertUserIsDenied("u6", "all", "c");
        assertAccessIsAllowed("u6",
                "GET", "/" + randomIndex() + "/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
        assertAccessIsAllowed("u6", "POST", "/" + randomIndex() + "/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
        assertAccessIsAllowed("u6", "PUT",
                "/" + randomIndex() + "/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsAllowed("u6",
                "GET", "/" + randomIndex() + "/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
    }

    public void testUserU7() throws Exception {
        // no access at all
        assertUserIsDenied("u7", "all", "a");
        assertUserIsDenied("u7", "all", "b");
        assertUserIsDenied("u7", "all", "c");
        assertAccessIsDenied("u7",
                "GET", "/" + randomIndex() + "/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
        assertAccessIsDenied("u7", "POST", "/" + randomIndex() + "/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
        assertAccessIsDenied("u7", "PUT",
                "/" + randomIndex() + "/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsDenied("u7",
                "GET", "/" + randomIndex() + "/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
    }

    public void testUserU8() throws Exception {
        // u8 has admin access and read access on b
        assertUserIsAllowed("u8", "all", "a");
        assertUserIsAllowed("u8", "all", "b");
        assertUserIsAllowed("u8", "all", "c");
        assertAccessIsAllowed("u8",
                "GET", "/" + randomIndex() + "/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
        assertAccessIsAllowed("u8", "POST", "/" + randomIndex() + "/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
        assertAccessIsAllowed("u8", "PUT",
                "/" + randomIndex() + "/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsAllowed("u8",
                "GET", "/" + randomIndex() + "/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
    }

    public void testUserU9() throws Exception {
        // u9 has write access to a and read access to a/b
        assertUserIsAllowed("u9", "crud", "a");
        assertUserIsDenied("u9", "manage", "a");
        assertUserIsAllowed("u9", "read", "b");
        assertUserIsDenied("u9", "manage", "b");
        assertUserIsDenied("u9", "write", "b");
        assertUserIsDenied("u9", "all", "c");
        assertAccessIsAllowed("u9",
                "GET", "/" + randomIndex() + "/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
        assertAccessIsAllowed("u9", "POST", "/" + randomIndex() + "/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
        assertAccessIsAllowed("u9", "PUT",
                "/" + randomIndex() + "/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsAllowed("u9",
                "GET", "/" + randomIndex() + "/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
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

        assertAccessIsDenied("u11",
                "GET", "/" + randomIndex() + "/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
        assertAccessIsDenied("u11", "POST", "/" + randomIndex() + "/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
        assertBodyHasAccessIsDenied("u11", "PUT",
                "/" + randomIndex() + "/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsDenied("u11",
                "GET", "/" + randomIndex() + "/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
    }

    public void testUserU12() throws Exception {
        // u12 has data_access to all indices+ crud access to a
        assertUserIsDenied("u12", "manage", "a");
        assertUserIsAllowed("u12", "data_access", "a");
        assertUserIsDenied("u12", "manage", "b");
        assertUserIsAllowed("u12", "data_access", "b");
        assertUserIsDenied("u12", "manage", "c");
        assertUserIsAllowed("u12", "data_access", "c");
        assertAccessIsAllowed("u12",
                "GET", "/" + randomIndex() + "/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
        assertAccessIsAllowed("u12", "POST", "/" + randomIndex() + "/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
        assertAccessIsAllowed("u12", "PUT",
                "/" + randomIndex() + "/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsAllowed("u12",
                "GET", "/" + randomIndex() + "/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
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

        assertAccessIsAllowed("u13",
                "GET", "/" + randomIndex() + "/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
        assertAccessIsAllowed("u13", "POST", "/" + randomIndex() + "/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
        assertAccessIsAllowed("u13", "PUT", "/a/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertBodyHasAccessIsDenied("u13", "PUT", "/b/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsAllowed("u13",
                "GET", "/" + randomIndex() + "/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
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

        assertAccessIsAllowed("u14",
                "GET", "/" + randomIndex() + "/_msearch", "{}\n{ \"query\" : { \"match_all\" : {} } }\n");
        assertAccessIsAllowed("u14", "POST", "/" + randomIndex() + "/_mget", "{ \"ids\" : [ \"1\", \"2\" ] } ");
        assertAccessIsDenied("u14", "PUT",
                "/" + randomIndex() + "/_bulk", "{ \"index\" : { \"_id\" : \"123\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsAllowed("u14",
                "GET", "/" + randomIndex() + "/_mtermvectors", "{ \"docs\" : [ { \"_id\": \"1\" }, { \"_id\": \"2\" } ] }");
    }

    public void testThatUnknownUserIsRejectedProperly() throws Exception {
        try {
            Request request = new Request("GET", "/");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue("idonotexist", new SecureString("passwd".toCharArray())));
            request.setOptions(options);
            getRestClient().performRequest(request);
            fail("request should have failed");
        } catch(ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(401));
        }
    }

    private void assertUserExecutes(String user, String action, String index, boolean userIsAllowed) throws Exception {
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
                    assertNoTimeout(client().admin().cluster().prepareHealth(index).setWaitForGreenStatus().get());
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_refresh");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_analyze", "{ \"text\" : \"test\" }");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_flush");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_forcemerge");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_upgrade", null);
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_close");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_open");
                    assertAccessIsAllowed(user, "POST", "/" + index + "/_cache/clear");
                    // indexing a document to have the mapping available, and wait for green state to make sure index is created
                assertAccessIsAllowed("admin", "PUT", "/" + index + "/_doc/1", jsonDoc);
                    assertNoTimeout(client().admin().cluster().prepareHealth(index).setWaitForGreenStatus().get());
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_mapping/field/name");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_settings");
                } else {
                    assertAccessIsDenied(user, "DELETE", "/" + index);
                    assertUserIsDenied(user, "create_index", index);
                    assertAccessIsDenied(user, "POST", "/" + index + "/_refresh");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_analyze", "{ \"text\" : \"test\" }");
                    assertAccessIsDenied(user, "POST", "/" + index + "/_flush");
                    assertAccessIsDenied(user, "POST", "/" + index + "/_forcemerge");
                    assertAccessIsDenied(user, "POST", "/" + index + "/_upgrade", null);
                    assertAccessIsDenied(user, "POST", "/" + index + "/_close");
                    assertAccessIsDenied(user, "POST", "/" + index + "/_open");
                    assertAccessIsDenied(user, "POST", "/" + index + "/_cache/clear");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_mapping/field/name");
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
                assertAccessIsAllowed("admin", "GET", "/" + index + "/_doc/1");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_explain/1", "{ \"query\" : { \"match_all\" : {} } }");
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_termvectors/1");
                    assertUserIsAllowed(user, "search", index);
                } else {
                    assertAccessIsDenied(user, "GET", "/" + index + "/_count");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_search");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_explain/1", "{ \"query\" : { \"match_all\" : {} } }");
                    assertAccessIsDenied(user, "GET", "/" + index + "/_termvectors/1");
                    assertUserIsDenied(user, "search", index);
                }
                break;

            case "search" :
                if (userIsAllowed) {
                    assertAccessIsAllowed(user, "GET", "/" + index + "/_search");
                } else {
                    assertAccessIsDenied(user, "GET", "/" + index + "/_search");
                }
                break;

            case "get" :
                if (userIsAllowed) {
                assertAccessIsAllowed(user, "GET", "/" + index + "/_doc/1");
                } else {
                assertAccessIsDenied(user, "GET", "/" + index + "/_doc/1");
                }
                break;

            case "index" :
                if (userIsAllowed) {
                assertAccessIsAllowed(user, "PUT", "/" + index + "/_doc/321", "{ \"foo\" : \"bar\" }");
                assertAccessIsAllowed(user, "POST", "/" + index + "/_update/321", "{ \"doc\" : { \"foo\" : \"baz\" } }");
                } else {
                assertAccessIsDenied(user, "PUT", "/" + index + "/_doc/321", "{ \"foo\" : \"bar\" }");
                assertAccessIsDenied(user, "POST", "/" + index + "/_update/321", "{ \"doc\" : { \"foo\" : \"baz\" } }");
                }
                break;

            case "delete" :
                String jsonDoc = "{ \"name\" : \"docToDelete\"}";
            assertAccessIsAllowed("admin", "PUT", "/" + index + "/_doc/docToDelete", jsonDoc);
            assertAccessIsAllowed("admin", "PUT", "/" + index + "/_doc/docToDelete2", jsonDoc);
                if (userIsAllowed) {
                assertAccessIsAllowed(user, "DELETE", "/" + index + "/_doc/docToDelete");
                } else {
                assertAccessIsDenied(user, "DELETE", "/" + index + "/_doc/docToDelete");
                }
                break;

            case "write" :
                if (userIsAllowed) {
                    assertUserIsAllowed(user, "index", index);
                    assertUserIsAllowed(user, "delete", index);
                } else {
                    assertUserIsDenied(user, "index", index);
                    assertUserIsDenied(user, "delete", index);
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
