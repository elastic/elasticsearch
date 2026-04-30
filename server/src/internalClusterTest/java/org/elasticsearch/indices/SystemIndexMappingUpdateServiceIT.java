/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.indices.TestSystemIndexDescriptor.INDEX_NAME;
import static org.elasticsearch.indices.TestSystemIndexDescriptor.PRIMARY_INDEX_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SystemIndexMappingUpdateServiceIT extends ESIntegTestCase {

    @Before
    public void beforeEach() {
        TestSystemIndexDescriptor.useNewMappings.set(false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestSystemIndexPlugin.class);
    }

    /**
     * Check that if the SystemIndexManager finds a managed index with out-of-date mappings, then
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
     * Check that if the SystemIndexManager finds a managed index with mappings that claim to be newer than
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
     * Reproduces a bug where system index mappings are not updated after a major-version reindex migration.
     * <a href="https://github.com/elastic/elasticsearch/issues/144764"></a>
     */
    public void testSystemIndexManagerUpgradesMappingsOfReindexedIndex() throws Exception {
        internalCluster().startNodes(1);

        // Directly set up the post-migration state: a reindexed concrete index with old mappings,
        // and the original primary index name as an alias pointing to it.
        // Use MIGRATE_SYSTEM_INDEX_CAUSE so that TransportCreateIndexAction allows creating a system
        // index whose name is not the descriptor's primary index, mirroring SystemIndexMigrator.
        final String reindexedIndexName = PRIMARY_INDEX_NAME + SystemIndices.UPGRADED_INDEX_SUFFIX;
        assertAcked(
            indicesAdmin().create(
                new CreateIndexRequest(reindexedIndexName).cause(SystemIndices.MIGRATE_SYSTEM_INDEX_CAUSE)
                    .settings(TestSystemIndexDescriptor.SETTINGS)
                    .mapping(TestSystemIndexDescriptor.getOldMappings())
            ).actionGet()
        );
        ensureGreen(reindexedIndexName);
        assertAcked(
            indicesAdmin().aliases(
                new IndicesAliasesRequest().addAliasAction(
                    IndicesAliasesRequest.AliasActions.add().index(reindexedIndexName).alias(PRIMARY_INDEX_NAME)
                ).addAliasAction(IndicesAliasesRequest.AliasActions.add().index(reindexedIndexName).alias(INDEX_NAME))
            ).actionGet()
        );

        TestSystemIndexDescriptor.useNewMappings.set(true);
        triggerClusterStateUpdates();

        assertBusy(() -> assertMappingsAndSettings(TestSystemIndexDescriptor.getNewMappings(), reindexedIndexName));
    }

    /**
     * Ensures that we can clear any blocks that get set on managed system indices.
     *
     * See https://github.com/elastic/elasticsearch/issues/80814
     */
    public void testBlocksCanBeClearedFromManagedSystemIndices() throws Exception {
        internalCluster().startNodes(1);

        // Trigger the creation of the system index
        assertAcked(prepareCreate(INDEX_NAME));
        ensureGreen(INDEX_NAME);

        for (IndexMetadata.APIBlock blockType : IndexMetadata.APIBlock.values()) {
            enableIndexBlock(INDEX_NAME, blockType.settingName());
            updateIndexSettings(Settings.builder().put(blockType.settingName(), false), INDEX_NAME);
        }
    }

    /**
     * Performs a cluster state update in order to trigger any cluster state listeners - specifically, SystemIndexManager.
     */
    private void triggerClusterStateUpdates() {
        final String name = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        indicesAdmin().putTemplate(new PutIndexTemplateRequest(name).patterns(List.of(name))).actionGet();
    }

    private void assertMappingsAndSettings(String expectedMappings) {
        assertMappingsAndSettings(expectedMappings, PRIMARY_INDEX_NAME);
    }

    /**
     * Fetch the mappings and settings for {@link TestSystemIndexDescriptor#INDEX_NAME} and verify that they match the expected values.
     */
    private void assertMappingsAndSettings(String expectedMappings, String primaryIndexName) {
        final GetMappingsResponse getMappingsResponse = indicesAdmin().getMappings(new GetMappingsRequest().indices(INDEX_NAME))
            .actionGet();

        final Map<String, MappingMetadata> mappings = getMappingsResponse.getMappings();
        assertThat(
            "Expected mappings to contain a key for [" + primaryIndexName + "], but found: " + mappings.toString(),
            mappings.containsKey(primaryIndexName),
            equalTo(true)
        );
        final Map<String, Object> sourceAsMap = mappings.get(primaryIndexName).getSourceAsMap();

        assertThat(sourceAsMap, equalTo(XContentHelper.convertToMap(XContentType.JSON.xContent(), expectedMappings, false)));

        final GetSettingsResponse getSettingsResponse = indicesAdmin().getSettings(new GetSettingsRequest().indices(INDEX_NAME))
            .actionGet();

        final Settings actual = getSettingsResponse.getIndexToSettings().get(primaryIndexName);

        for (String settingName : TestSystemIndexDescriptor.SETTINGS.keySet()) {
            assertThat(actual.get(settingName), equalTo(TestSystemIndexDescriptor.SETTINGS.get(settingName)));
        }
    }

}
