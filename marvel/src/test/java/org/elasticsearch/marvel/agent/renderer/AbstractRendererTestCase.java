/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;


@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, randomDynamicTemplates = false, transportClientRatio = 0.0)
public abstract class AbstractRendererTestCase extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(Node.HTTP_ENABLED, true)
                .put(MarvelSettings.STARTUP_DELAY, "3s")
                .put(MarvelSettings.INTERVAL, "1s")
                .put(MarvelSettings.COLLECTORS, Strings.collectionToCommaDelimitedString(collectors()));

        // we need to remove this potential setting for shield
        builder.remove("index.queries.cache.type");

        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LicensePlugin.class, MarvelPlugin.class, ShieldPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    protected abstract Collection<String> collectors ();

    protected void waitForMarvelDocs(final String type) throws Exception {
        waitForMarvelDocs(type, 0L);
    }

    protected void waitForMarvelDocs(final String type, final long minCount) throws Exception {
        logger.debug("--> waiting for at least [{}] marvel docs of type [{}] to be collected", minCount, type);
        assertBusy(new Runnable() {
            @Override
            public void run() {
                try {
                    refresh();
                    assertThat(client().prepareCount().setTypes(type).get().getCount(), greaterThan(minCount));
                } catch (Throwable t) {
                    fail("exception when waiting for marvel docs: " + t.getMessage());
                }
            }
        }, 30L, TimeUnit.SECONDS);
    }

    /**
     * Checks if a field exist in a map of values. If the field contains a dot like 'foo.bar'
     * it checks that 'foo' exists in the map of values and that it points to a sub-map. Then
     * it recurses to check if 'bar' exists in the sub-map.
     */
    protected void assertContains(String field, Map<String, Object> values) {
        assertNotNull("field name should not be null", field);
        assertNotNull("values map should not be null", values);

        int point = field.indexOf('.');
        if (point > -1) {
            assertThat(point, allOf(greaterThan(0), lessThan(field.length())));

            String segment = field.substring(0, point);
            assertTrue(Strings.hasText(segment));

            boolean fieldExists = values.containsKey(segment);
            assertTrue("expecting field [" + segment + "] to be present in marvel document", fieldExists);

            Object value = values.get(segment);
            String next = field.substring(point + 1);
            if (next.length() > 0) {
                assertTrue(value instanceof Map);
                assertContains(next, (Map<String, Object>) value);
            } else {
                assertFalse(value instanceof Map);
            }
        } else {
            assertTrue("expecting field [" + field + "] to be present in marvel document", values.containsKey(field));
        }
    }

    protected void assertMarvelTemplateExists() throws Exception {
        final String marvelTemplate = "marvel";

        assertBusy(new Runnable() {
            @Override
            public void run() {
                GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates(marvelTemplate).get();
                assertNotNull(response);

                boolean found = false;
                for (IndexTemplateMetaData template : response.getIndexTemplates()) {
                    if (marvelTemplate.equals(template.getName())) {
                        found = true;
                        break;
                    }
                }
                assertTrue("Template [" + marvelTemplate + "] not found", found);
            }
        });
    }
}
