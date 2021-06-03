/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.indices.TestSystemIndexDescriptor.INDEX_NAME;
import static org.elasticsearch.indices.TestSystemIndexDescriptor.PRIMARY_INDEX_NAME;
import static org.elasticsearch.test.XContentTestUtils.convertToXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SystemIndexManagerIT extends ESIntegTestCase {

    @Before
    public void beforeEach() {
        TestSystemIndexDescriptor.useNewMappings.set(false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestSystemIndexPlugin.class);
    }

    /**
     * Check that if the the SystemIndexManager finds a managed index with out-of-date mappings, then
     * the manager updates those mappings.
     */
    public void testSystemIndexManagerUpgradesMappings() throws Exception {
        internalCluster().startNodes(1);

        // Trigger the creation of the system index
        assertAcked(prepareCreate(INDEX_NAME));
        ensureGreen(INDEX_NAME);

        assertMappingsAndSettings(TestSystemIndexDescriptor.getOldMappings());

        // Poke the test descriptor so that the mappings are now "updated"
        TestSystemIndexDescriptor.useNewMappings.set(true);

        // Cause a cluster state update, so that the SystemIndexManager will update the mappings in our index
        triggerClusterStateUpdates();

        assertBusy(() -> assertMappingsAndSettings(TestSystemIndexDescriptor.getNewMappings()));
    }

    /**
     * Check that if the the SystemIndexManager finds a managed index with mappings that claim to be newer than
     * what it expects, then those mappings are left alone.
     */
    public void testSystemIndexManagerLeavesNewerMappingsAlone() throws Exception {
        TestSystemIndexDescriptor.useNewMappings.set(true);

        internalCluster().startNodes(1);
        // Trigger the creation of the system index
        assertAcked(prepareCreate(INDEX_NAME));
        ensureGreen(INDEX_NAME);

        assertMappingsAndSettings(TestSystemIndexDescriptor.getNewMappings());

        // Poke the test descriptor so that the mappings are now out-dated.
        TestSystemIndexDescriptor.useNewMappings.set(false);

        // Cause a cluster state update, so that the SystemIndexManager's listener will execute
        triggerClusterStateUpdates();

        // Mappings should be unchanged.
        assertBusy(() -> assertMappingsAndSettings(TestSystemIndexDescriptor.getNewMappings()));
    }

    /**
     * Performs a cluster state update in order to trigger any cluster state listeners - specifically, SystemIndexManager.
     */
    private void triggerClusterStateUpdates() {
        final String name = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        client().admin().indices().putTemplate(new PutIndexTemplateRequest(name).patterns(List.of(name))).actionGet();
    }

    /**
     * Fetch the mappings and settings for {@link TestSystemIndexDescriptor#INDEX_NAME} and verify that they match the expected values.
     * Note that in the case of the mappings, this is just a dumb string comparison, so order of keys matters.
     */
    private void assertMappingsAndSettings(String expectedMappings) {
        final GetMappingsResponse getMappingsResponse = client().admin()
            .indices()
            .getMappings(new GetMappingsRequest().indices(INDEX_NAME))
            .actionGet();

        final ImmutableOpenMap<String, MappingMetadata> mappings = getMappingsResponse.getMappings();
        assertThat(
            "Expected mappings to contain a key for [" + PRIMARY_INDEX_NAME + "], but found: " + mappings.toString(),
            mappings.containsKey(PRIMARY_INDEX_NAME),
            equalTo(true)
        );
        final Map<String, Object> sourceAsMap = mappings.get(PRIMARY_INDEX_NAME).getSourceAsMap();

        try {
            assertThat(convertToXContent(sourceAsMap, XContentType.JSON).utf8ToString(), equalTo(expectedMappings));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        final GetSettingsResponse getSettingsResponse = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(INDEX_NAME))
            .actionGet();

        final Settings actual = getSettingsResponse.getIndexToSettings().get(PRIMARY_INDEX_NAME);

        for (String settingName : TestSystemIndexDescriptor.SETTINGS.keySet()) {
            assertThat(actual.get(settingName), equalTo(TestSystemIndexDescriptor.SETTINGS.get(settingName)));
        }
    }

}
