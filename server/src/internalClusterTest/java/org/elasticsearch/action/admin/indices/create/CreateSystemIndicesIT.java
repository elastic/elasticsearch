/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.indices.TestSystemIndexDescriptor;
import org.elasticsearch.indices.TestSystemIndexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.indices.TestSystemIndexDescriptor.INDEX_NAME;
import static org.elasticsearch.indices.TestSystemIndexDescriptor.PRIMARY_INDEX_NAME;
import static org.elasticsearch.test.XContentTestUtils.convertToXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CreateSystemIndicesIT extends ESIntegTestCase {

    @Before
    public void beforeEach() {
        TestSystemIndexDescriptor.useNewMappings.set(false);
        assertAcked(client().admin().indices().prepareDeleteTemplate("*").get());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestSystemIndexPlugin.class);
    }

    /**
     * Check that a system index is auto-created with the expected mappings and
     * settings when it is first used, when it is referenced via its alias.
     */
    public void testSystemIndexIsAutoCreatedViaAlias() {
        doCreateTest(() -> indexDoc(INDEX_NAME, "1", "foo", "bar"), PRIMARY_INDEX_NAME);
    }

    /**
     * Check that a system index is auto-created with the expected mappings and
     * settings when it is first used, when it is referenced via its concrete
     * index name.
     */
    public void testSystemIndexIsAutoCreatedViaConcreteName() {
        doCreateTest(() -> indexDoc(PRIMARY_INDEX_NAME, "1", "foo", "bar"), PRIMARY_INDEX_NAME);
    }

    /**
     * Check that a system index is auto-created with the expected mappings and
     * settings when it is first used, when it is referenced via its concrete
     * index name.
     */
    public void testNonPrimarySystemIndexIsAutoCreatedViaConcreteName() {
        final String nonPrimarySystemIndex = INDEX_NAME + "-2";
        doCreateTest(() -> indexDoc(nonPrimarySystemIndex, "1", "foo", "bar"), nonPrimarySystemIndex);
    }

    /**
     * Check that a system index is created with the expected mappings and
     * settings when it is explicitly created, when it is referenced via its alias.
     */
    public void testCreateSystemIndexViaAlias() {
        doCreateTest(() -> assertAcked(prepareCreate(INDEX_NAME)), PRIMARY_INDEX_NAME);
    }

    /**
     * Check that a system index is created with the expected mappings and
     * settings when it is explicitly created, when it is referenced via its
     * concrete index name.
     */
    public void testCreateSystemIndexViaConcreteName() {
        doCreateTest(() -> assertAcked(prepareCreate(PRIMARY_INDEX_NAME)), PRIMARY_INDEX_NAME);
    }

    /**
     * Check that a template applying a system alias creates a hidden alias.
     */
    public void testCreateSystemAliasViaV1Template() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .preparePutTemplate("test-template")
                .setPatterns(List.of(INDEX_NAME + "*"))
                .addAlias(new Alias(INDEX_NAME + "-alias"))
                .get()
        );

        internalCluster().startNodes(1);

        assertAcked(prepareCreate(INDEX_NAME + "-2"));
        ensureGreen(INDEX_NAME); // huh?

        final GetAliasesResponse getAliasesResponse = client().admin()
            .indices()
            .getAliases(new GetAliasesRequest().indicesOptions(IndicesOptions.strictExpandHidden()))
            .get();

        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get(PRIMARY_INDEX_NAME).size(), equalTo(2));
        assertThat(getAliasesResponse.getAliases().get(PRIMARY_INDEX_NAME).get(0).isHidden(), equalTo(true));
        assertThat(getAliasesResponse.getAliases().get(PRIMARY_INDEX_NAME).get(1).isHidden(), equalTo(true));

        assertAcked(client().admin().indices().prepareDeleteTemplate("*").get());
    }

    private void doCreateTest(Runnable runnable, String concreteIndex) {
        internalCluster().startNodes(1);

        // Trigger the creation of the system index
        runnable.run();
        ensureGreen(INDEX_NAME);

        assertMappingsAndSettings(TestSystemIndexDescriptor.getOldMappings(), concreteIndex);

        // Remove the index and alias...
        assertAcked(client().admin().indices().prepareAliases().removeAlias(concreteIndex, INDEX_NAME).get());
        assertAcked(client().admin().indices().prepareDelete(concreteIndex));

        // ...so that we can check that the they will still be auto-created again,
        // but this time with updated settings
        TestSystemIndexDescriptor.useNewMappings.set(true);

        runnable.run();
        ensureGreen(INDEX_NAME);

        assertMappingsAndSettings(TestSystemIndexDescriptor.getNewMappings(), concreteIndex);
        assertAliases(concreteIndex);
    }

    /**
     * Make sure that aliases are created hidden
     */
    private void assertAliases(String concreteIndex) {
        final GetAliasesResponse getAliasesResponse = client().admin()
            .indices()
            .getAliases(new GetAliasesRequest().indicesOptions(IndicesOptions.strictExpandHidden()))
            .actionGet();

        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get(concreteIndex).size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get(concreteIndex).get(0).isHidden(), equalTo(true));
    }

    /**
     * Fetch the mappings and settings for {@link TestSystemIndexDescriptor#INDEX_NAME} and verify that they match the expected values.
     * Note that in the case of the mappings, this is just a dumb string comparison, so order of keys matters.
     */
    private void assertMappingsAndSettings(String expectedMappings, String concreteIndex) {
        final GetMappingsResponse getMappingsResponse = client().admin()
            .indices()
            .getMappings(new GetMappingsRequest().indices(INDEX_NAME))
            .actionGet();

        final ImmutableOpenMap<String, MappingMetadata> mappings = getMappingsResponse.getMappings();
        assertThat(
            "Expected mappings to contain a key for [" + concreteIndex + "], but found: " + mappings.toString(),
            mappings.containsKey(concreteIndex),
            equalTo(true)
        );
        final Map<String, Object> sourceAsMap = mappings.get(concreteIndex).getSourceAsMap();

        try {
            assertThat(convertToXContent(sourceAsMap, XContentType.JSON).utf8ToString(), equalTo(expectedMappings));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        final GetSettingsResponse getSettingsResponse = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(INDEX_NAME))
            .actionGet();

        final Settings actual = getSettingsResponse.getIndexToSettings().get(concreteIndex);

        for (String settingName : TestSystemIndexDescriptor.SETTINGS.keySet()) {
            assertThat(actual.get(settingName), equalTo(TestSystemIndexDescriptor.SETTINGS.get(settingName)));
        }
    }

}
