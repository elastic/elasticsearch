/*
 *
 *  * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 *  * or more contributor license agreements. Licensed under the Elastic License;
 *  * you may not use this file except in compliance with the Elastic License.
 *
 */

package org.elasticsearch.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.junit.Before;

import java.io.IOException;

public class AppendOnlyIndexPrivilegeTests extends AbstractPrivilegeTestCase {
    private static final String INDEX_NAME = "index-1";
    private String jsonDoc = "{ \"name\" : \"elasticsearch\", \"body\": \"foo bar\" }";
    private static final String ROLES =
            "all_indices_role:\n" +
            "  indices:\n" +
            "    - names: '*'\n" +
            "      privileges: [ all ]\n" +
            "append_only_role:\n" +
            "  indices:\n" +
            "    - names: '*'\n" +
            "      privileges: [ append_only ]\n"
            ;

    private static final String USERS_ROLES =
        "all_indices_role:admin\n" +
        "append_only_role:append_only_user\n";

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
            "append_only_user:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + USERS_ROLES;
    }

    @Before
    public void insertBaseDocumentsAsAdmin() throws Exception {
        Request request = new Request("PUT", "/" + INDEX_NAME + "/_doc/1");
        request.setJsonEntity(jsonDoc);
        request.addParameter("refresh", "true");
        assertAccessIsAllowed("admin", request);
    }

    public void testAppendOnlyUser() throws IOException {
        String user = "append_only_user";
        // append_only_user must be able to index a new document via Index API with auto generate id
        assertAccessIsAllowed(user, "POST", "/" + INDEX_NAME + "/_doc", "{ \"foo\" : \"bar\" }");
        // append_only_user is allowed to index document if it does not exist when Id is specified and op_type is create
        assertAccessIsAllowed(user, "PUT", "/" + INDEX_NAME + "/_doc/123?op_type=create", "{ \"foo\" : \"bar\" }");

        // append_only_user is not allowed to index document even if it does not exist when Id is specified and op_type is default (index)
        assertAccessIsDenied(user, "PUT", "/" + INDEX_NAME + "/_doc/321", "{ \"foo\" : \"bar\" }");
        // no updates allowed
        assertAccessIsDenied(user, "POST", "/" + INDEX_NAME + "/_doc/1/_update", "{ \"doc\" : { \"foo\" : \"baz\" } }");
        assertAccessIsDenied(user, "PUT", "/" + INDEX_NAME + "/_doc/1", "{ \"foo\" : \"baz\" }");

        // bulk API with create and index with no id only
        assertAccessIsAllowed(user, randomFrom("PUT", "POST"),
            "/" + INDEX_NAME + "/_bulk", "{ \"create\" : { \"_id\" : \"145\" } }\n{ \"foo\" : \"bar\" }\n");
        assertAccessIsAllowed(user, randomFrom("PUT", "POST"),
            "/" + INDEX_NAME + "/_bulk", "{ \"index\" : { } }\n{ \"foo\" : \"bar\" }\n");

        // Bulk API deny access for update or index with id (we do not know if it exists)
        assertBodyHasAccessIsDenied(user, randomFrom("PUT", "POST"),
            "/" + INDEX_NAME + "/_bulk", "{ \"update\" : { \"_id\" : \"145\" } }\n{ \"doc\" : {\"foo\" : \"bazbaz\"} }\n");
        assertBodyHasAccessIsDenied(user, randomFrom("PUT", "POST"),
            "/" + INDEX_NAME + "/_bulk", "{ \"index\" : { \"_id\" : \"149\" } }\n{ \"foo\" : \"bar\" }\n");
    }
}
