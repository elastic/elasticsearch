/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.audit.index.IndexAuditTrail;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class IndexAuditIT extends ESIntegTestCase {
    private static final String USER = "test_user";
    private static final String PASS = "changeme";

    public void testShieldIndexAuditTrailWorking() throws Exception {
        HttpResponse response = httpClient().path("/")
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(USER, new SecuredString(PASS.toCharArray())))
                .execute();
        assertThat(response.getStatusCode(), is(200));

        final AtomicReference<ClusterState> lastClusterState = new AtomicReference<>();
        final AtomicBoolean indexExists = new AtomicBoolean(false);
        boolean found = awaitBusy(() -> {
            if (indexExists.get() == false) {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                lastClusterState.set(state);
                for (ObjectCursor<String> cursor : state.getMetaData().getIndices().keys()) {
                    if (cursor.value.startsWith(".shield_audit_log")) {
                        logger.info("found audit index [{}]", cursor.value);
                        indexExists.set(true);
                        break;
                    }
                }

                if (indexExists.get() == false) {
                    return false;
                }
            }

            ensureYellow(".shield_audit_log*");
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            lastClusterState.set(state);
            client().admin().indices().prepareRefresh().get();
            return client().prepareSearch(".shield_audit_log*").setQuery(QueryBuilders.matchQuery("principal", USER))
                    .get().getHits().totalHits() > 0;
        }, 10L, TimeUnit.SECONDS);

        if (!found) {
            logger.info("current cluster state: {}", lastClusterState.get());
        }
        assertThat(found, is(true));

        SearchResponse searchResponse = client().prepareSearch(".shield_audit_log*").setQuery(
                QueryBuilders.matchQuery("principal", USER)).get();
        assertThat(searchResponse.getHits().getHits().length, greaterThan(0));
        assertThat((String) searchResponse.getHits().getAt(0).sourceAsMap().get("principal"), is(USER));
    }

    public void testAuditTrailTemplateIsRecreatedAfterDelete() throws Exception {
        // this is already "tested" by the test framework since we wipe the templates before and after,
        // but lets be explicit about the behavior
        awaitIndexTemplateCreation();

        // delete the template
        DeleteIndexTemplateResponse deleteResponse = client().admin().indices()
                .prepareDeleteTemplate(IndexAuditTrail.INDEX_TEMPLATE_NAME).execute().actionGet();
        assertThat(deleteResponse.isAcknowledged(), is(true));
        awaitIndexTemplateCreation();
    }

    private void awaitIndexTemplateCreation() throws InterruptedException {
        boolean found = awaitBusy(() -> {
            GetIndexTemplatesResponse response = client().admin().indices()
                    .prepareGetTemplates(IndexAuditTrail.INDEX_TEMPLATE_NAME).execute().actionGet();
            if (response.getIndexTemplates().size() > 0) {
                for (IndexTemplateMetaData indexTemplateMetaData : response.getIndexTemplates()) {
                    if (IndexAuditTrail.INDEX_TEMPLATE_NAME.equals(indexTemplateMetaData.name())) {
                        return true;
                    }
                }
            }
            return false;
        });

        assertThat("index template [" + IndexAuditTrail.INDEX_TEMPLATE_NAME + "] was not created", found, is(true));
    }

    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder()
                .put(Security.USER_SETTING.getKey(), USER + ":" + PASS)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.<Class<? extends Plugin>>singleton(XPackPlugin.class);
    }
}
