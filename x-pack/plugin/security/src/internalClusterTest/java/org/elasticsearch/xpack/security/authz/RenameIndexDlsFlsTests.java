/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.junit.After;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

/**
 * Document and field level security on an index that was renamed. The reader of a shard keeps the name the shard had when it was
 * created, while the request is authorized for the name the index has now, so the restrictions have to be found by the index's
 * current name or they are not applied at all.
 */
public class RenameIndexDlsFlsTests extends SecurityIntegTestCase {

    @After
    public void cleanupSecurityIndex() {
        super.deleteSecurityIndex();
    }

    @Override
    protected String configUsers() {
        final String hashed = SecuritySettingsSource.TEST_PASSWORD_HASHED;
        return super.configUsers() + "dls_user:" + hashed + "\n" + "fls_user:" + hashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + "dls_role:dls_user\n" + "fls_role:fls_user\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + """

            dls_role:
              indices:
                - names: 'dls*'
                  privileges: [ read ]
                  query: '{"term": {"visible": true}}'
            fls_role:
              indices:
                - names: 'fls*'
                  privileges: [ read ]
                  field_security:
                    grant: [ visible ]
            """;
    }

    public void testDocumentLevelSecurityAppliesAfterRename() {
        createIndex("dls-old", 1, 0, false);
        indexDoc("dls-old", "1", "visible", true, "n", 1);
        indexDoc("dls-old", "2", "visible", false, "n", 2);
        indexDoc("dls-old", "3", "visible", false, "n", 3);
        refresh("dls-old");

        assertResponse(
            clientAs("dls_user").prepareSearch("dls-old"),
            response -> assertThat("the restriction works before the rename", response.getHits().getTotalHits().value(), equalTo(1L))
        );

        rename("dls-old", "dls-new");

        assertResponse(
            clientAs("dls_user").prepareSearch("dls-new"),
            response -> assertThat("documents hidden by the role are returned", response.getHits().getTotalHits().value(), equalTo(1L))
        );
        // new documents get into segments the reader sees after a refresh, which does not renew the shard id it carries
        indexDoc("dls-new", "4", "visible", false, "n", 4);
        indexDoc("dls-new", "5", "visible", true, "n", 5);
        refresh("dls-new");
        assertResponse(
            clientAs("dls_user").prepareSearch("dls-new"),
            response -> assertThat(response.getHits().getTotalHits().value(), equalTo(2L))
        );
    }

    public void testFieldLevelSecurityAppliesAfterRename() {
        createIndex("fls-old", 1, 0, false);
        indexDoc("fls-old", "1", "visible", "v", "secret", "s");
        refresh("fls-old");

        assertResponse(clientAs("fls_user").prepareSearch("fls-old"), response -> {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(
                "the restriction works before the rename",
                response.getHits().getAt(0).getSourceAsMap().keySet().contains("secret"),
                equalTo(false)
            );
        });

        rename("fls-old", "fls-new");

        assertResponse(clientAs("fls_user").prepareSearch("fls-new"), response -> {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(
                "a field the role does not grant is returned",
                response.getHits().getAt(0).getSourceAsMap().keySet().contains("secret"),
                equalTo(false)
            );
        });
    }

    private void rename(String source, String target) {
        assertTrue(client().execute(TransportIndicesAliasesAction.TYPE, renameIndexRequest(source, target)).actionGet().isAcknowledged());
    }

    private Client clientAs(String user) {
        return client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING))
        );
    }
}
