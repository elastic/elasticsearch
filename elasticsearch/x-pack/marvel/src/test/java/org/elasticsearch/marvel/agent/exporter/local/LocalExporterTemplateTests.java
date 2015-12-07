/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.marvel.agent.exporter.AbstractExporterTemplateTestCase;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

public class LocalExporterTemplateTests extends AbstractExporterTemplateTestCase {

    @Override
    protected Settings exporterSettings() {
        return Settings.builder().put("type", LocalExporter.TYPE).build();
    }

    @Override
    protected Set<String> excludeTemplates() {
        // Always delete the template between tests
        return Collections.emptySet();
    }

    @Override
    protected void deleteTemplate() throws Exception {
        waitNoPendingTasksOnAll();
        assertAcked(client().admin().indices().prepareDeleteTemplate(MarvelTemplateUtils.INDEX_TEMPLATE_NAME).get());
    }

    @Override
    protected void putTemplate(String version) throws Exception {
        waitNoPendingTasksOnAll();
        assertAcked(client().admin().indices().preparePutTemplate(MarvelTemplateUtils.INDEX_TEMPLATE_NAME).setSource(generateTemplateSource(version)).get());
    }

    @Override
    protected void createMarvelIndex(String index) throws Exception {
        waitNoPendingTasksOnAll();
        createIndex(index);
    }

    @Override
    protected void assertTemplateUpdated(Version version) throws Exception {
        waitNoPendingTasksOnAll();
        awaitMarvelTemplateInstalled(version);
    }

    @Override
    protected void assertTemplateNotUpdated(Version version) throws Exception {
        waitNoPendingTasksOnAll();
        awaitMarvelTemplateInstalled(version);
    }

    private void assertMappings(byte[] reference, String... indices) throws Exception {
        waitNoPendingTasksOnAll();

        Map<String, String> mappings = new PutIndexTemplateRequest().source(reference).mappings();
        assertBusy(new Runnable() {
            @Override
            public void run() {
                for (String index : indices) {
                    GetMappingsResponse response = client().admin().indices().prepareGetMappings(index).setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
                    ImmutableOpenMap<String, MappingMetaData> indexMappings = response.getMappings().get(index);
                    assertNotNull(indexMappings);
                    assertThat(indexMappings.size(), equalTo(mappings.size()));

                    for (String mapping : mappings.keySet()) {
                        // We just check that mapping type exists, we don't verify its content
                        assertThat("mapping type " + mapping + " should exist in index " + index, indexMappings.get(mapping), notNullValue());
                    }
                }
            }
        });
    }

    @Override
    protected void assertMappingsUpdated(String... indices) throws Exception {
        assertMappings(MarvelTemplateUtils.loadDefaultTemplate(), indices);
    }

    @Override
    protected void assertMappingsNotUpdated(String... indices) throws Exception {
        assertMappings(generateTemplateSource(null).toBytes(), indices);
    }

    @Override
    protected void assertIndicesNotCreated() throws Exception {
        waitNoPendingTasksOnAll();
        try {
            assertThat(client().admin().indices().prepareExists(MarvelSettings.MARVEL_INDICES_PREFIX + "*").get().isExists(), is(false));
        } catch (IndexNotFoundException e) {
            // with shield we might get that if wildcards were resolved to no indices
            if (!shieldEnabled) {
                throw e;
            }
        }
    }
}
