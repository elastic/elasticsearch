/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.util.Locale;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.Matchers.is;

public class KibanaSystemRoleIntegTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = new SecureString("change_me".toCharArray());

    @Override
    public String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));
        return super.configUsers() +
            "kibana_system:" + usersPasswdHashed;
    }

    @Override
    public String configUsersRoles() {
        return super.configUsersRoles() +
                "kibana_system:kibana_system";
    }


    public void testCreateIndexDeleteInKibanaIndex() throws Exception {
        final String index = randomBoolean()? ".kibana" : ".kibana-" + randomAlphaOfLengthBetween(1, 10).toLowerCase(Locale.ENGLISH);

        if (randomBoolean()) {
            CreateIndexResponse createIndexResponse = client().filterWithHeader(singletonMap("Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue("kibana_system", USERS_PASSWD)))
                    .admin().indices().prepareCreate(index).get();
            assertThat(createIndexResponse.isAcknowledged(), is(true));
        }

        IndexResponse response = client()
                .filterWithHeader(singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_system", USERS_PASSWD)))
                .prepareIndex()
                .setIndex(index)
                .setSource("foo", "bar")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        assertEquals(DocWriteResponse.Result.CREATED, response.getResult());

        DeleteResponse deleteResponse = client()
                .filterWithHeader(singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_system", USERS_PASSWD)))
                .prepareDelete(index, response.getId())
                .get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
    }
}
