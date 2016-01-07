/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.shield;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

public class SecuredClientTests extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(Node.HTTP_ENABLED, false)
                .put(MarvelSettings.INTERVAL_SETTING.getKey(), "-1")
                .build();
    }

    public void testAllowedAccess() {
        SecuredClient securedClient = internalCluster().getInstance(SecuredClient.class);

        assertAccessIsAllowed(securedClient.admin().cluster().prepareHealth());
        assertAccessIsAllowed(securedClient.admin().cluster().prepareClusterStats());
        assertAccessIsAllowed(securedClient.admin().cluster().prepareState());
        assertAccessIsAllowed(securedClient.admin().cluster().prepareNodesInfo());
        assertAccessIsAllowed(securedClient.admin().cluster().prepareNodesStats());
        assertAccessIsAllowed(securedClient.admin().cluster().prepareNodesHotThreads());

        assertAccessIsAllowed(securedClient.admin().indices().prepareGetSettings());
        assertAccessIsAllowed(securedClient.admin().indices().prepareSegments());
        assertAccessIsAllowed(securedClient.admin().indices().prepareRecoveries());
        assertAccessIsAllowed(securedClient.admin().indices().prepareStats());

        assertAccessIsAllowed(securedClient.admin().indices().prepareDelete(MarvelSettings.MARVEL_INDICES_PREFIX));
        assertAccessIsAllowed(securedClient.admin().indices().prepareCreate(MarvelSettings.MARVEL_INDICES_PREFIX + "test"));

        assertAccessIsAllowed(securedClient.admin().indices().preparePutTemplate("foo").setSource(MarvelTemplateUtils.loadTimestampedIndexTemplate()));
        assertAccessIsAllowed(securedClient.admin().indices().prepareGetTemplates("foo"));
    }

    public void testDeniedAccess() {
        SecuredClient securedClient = internalCluster().getInstance(SecuredClient.class);
        assertAcked(securedClient.admin().indices().preparePutTemplate("foo").setSource(MarvelTemplateUtils.loadDataIndexTemplate()).get());

        if (shieldEnabled) {
            assertAccessIsDenied(securedClient.admin().indices().prepareDeleteTemplate("foo"));
            assertAccessIsDenied(securedClient.admin().cluster().prepareGetRepositories());
        } else {
            assertAccessIsAllowed(securedClient.admin().indices().prepareDeleteTemplate("foo"));
            assertAccessIsAllowed(securedClient.admin().cluster().prepareGetRepositories());
        }
    }

    public void assertAccessIsAllowed(ActionRequestBuilder request) {
        try {
            request.get();
        } catch (IndexNotFoundException e) {
            // Ok
        } catch (ElasticsearchSecurityException e) {
            fail("unexpected security exception: " + e.getMessage());
        }
    }

    public void assertAccessIsDenied(ActionRequestBuilder request) {
        try {
            request.get();
            fail("expected a security exception");
        } catch (IndexNotFoundException e) {
            // Ok
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
        }
    }
}

