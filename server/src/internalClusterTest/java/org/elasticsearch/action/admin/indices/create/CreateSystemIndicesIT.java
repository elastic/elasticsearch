/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.TestSystemIndexDescriptor;
import org.elasticsearch.indices.TestSystemIndexDescriptorAllowsTemplates;
import org.elasticsearch.indices.TestSystemIndexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.TestSystemIndexDescriptor.INDEX_NAME;
import static org.elasticsearch.indices.TestSystemIndexDescriptor.PRIMARY_INDEX_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class CreateSystemIndicesIT extends ESIntegTestCase {

    @Before
    public void beforeEach() {
        TestSystemIndexDescriptor.useNewMappings.set(false);
    }

    @After
    public void afterEach() throws Exception {
        assertAcked(indicesAdmin().prepareDeleteTemplate("*").get());
        assertAcked(
            client().execute(
                TransportDeleteComposableIndexTemplateAction.TYPE,
                new TransportDeleteComposableIndexTemplateAction.Request("*")
            )
        );
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
    public void testNonPrimarySystemIndexIsAutoCreatedViaConcreteName() throws Exception {
        final String nonPrimarySystemIndex = INDEX_NAME + "-2";

        // Trigger the creation of the system index
        indexDoc(nonPrimarySystemIndex, "1", "foo", "bar");
        ensureGreen(nonPrimarySystemIndex);

        assertFalse(indexExists(PRIMARY_INDEX_NAME));
        assertTrue(indexExists(INDEX_NAME + "-2"));

        // Check that a non-primary system index is not assigned as the write index for the alias
        final GetAliasesResponse getAliasesResponse = indicesAdmin().getAliases(
            new GetAliasesRequest(TEST_REQUEST_TIMEOUT).indicesOptions(IndicesOptions.strictExpandHidden())
        ).actionGet();

        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get(nonPrimarySystemIndex).size(), equalTo(1));
        assertThat(
            getAliasesResponse.getAliases().get(nonPrimarySystemIndex).get(0),
            equalTo(AliasMetadata.builder(INDEX_NAME).isHidden(true).build())
        );
    }

    /**
     * This is weird behavior, but it's what we have. You can autocreate a non-primary system index,
     * but you can't directly create one.
     */
    public void testNonPrimarySystemIndexCreationThrowsError() {
        final String nonPrimarySystemIndex = INDEX_NAME + "-2";

        // Create the system index
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createIndex(nonPrimarySystemIndex));
        assertThat(
            e.getMessage(),
            equalTo("Cannot create system index with name " + nonPrimarySystemIndex + "; descriptor primary index is " + PRIMARY_INDEX_NAME)
        );
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

    private void createSystemAliasViaV1Template(String indexName, String primaryIndexName) throws Exception {
        assertAcked(
            indicesAdmin().preparePutTemplate("test-template")
                .setPatterns(List.of(indexName + "*"))
                .addAlias(new Alias(indexName + "-legacy-alias"))
        );

        assertAcked(prepareCreate(primaryIndexName));
        ensureGreen(primaryIndexName);
    }

    /**
     * Check that a legacy template does not create an alias for a system index
     */
    public void testCreateSystemAliasViaV1Template() throws Exception {
        createSystemAliasViaV1Template(INDEX_NAME, PRIMARY_INDEX_NAME);

        assertHasAliases(Set.of(INDEX_NAME), INDEX_NAME, PRIMARY_INDEX_NAME, 1);
    }

    /**
     * Check that a legacy template does create an alias for a system index because of allows templates
     */
    public void testCreateSystemAliasViaV1TemplateAllowsTemplates() throws Exception {
        createSystemAliasViaV1Template(
            TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME,
            TestSystemIndexDescriptorAllowsTemplates.PRIMARY_INDEX_NAME
        );

        assertHasAliases(
            Set.of(
                TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME,
                TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME + "-legacy-alias"
            ),
            TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME,
            TestSystemIndexDescriptorAllowsTemplates.PRIMARY_INDEX_NAME,
            2
        );
    }

    private void createIndexWithComposableTemplates(String indexName, String primaryIndexName) throws Exception {
        ComposableIndexTemplate cit = ComposableIndexTemplate.builder()
            .indexPatterns(Collections.singletonList(indexName + "*"))
            .template(
                new Template(
                    null,
                    null,
                    Map.of(indexName + "-composable-alias", AliasMetadata.builder(indexName + "-composable-alias").build())
                )
            )
            .componentTemplates(Collections.emptyList())
            .priority(4L)
            .version(5L)
            .metadata(Collections.emptyMap())
            .build();
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("test-composable-template").indexTemplate(cit)
            )
        );

        assertAcked(prepareCreate(primaryIndexName));
        ensureGreen(primaryIndexName);
    }

    /**
     * Check that a composable template does not create an alias for a system index
     */
    public void testCreateSystemAliasViaComposableTemplate() throws Exception {
        createIndexWithComposableTemplates(INDEX_NAME, PRIMARY_INDEX_NAME);

        assertHasAliases(Set.of(INDEX_NAME), INDEX_NAME, PRIMARY_INDEX_NAME, 1);

        assertAcked(
            client().execute(
                TransportDeleteComposableIndexTemplateAction.TYPE,
                new TransportDeleteComposableIndexTemplateAction.Request("test-composable-template")
            )
        );
    }

    /**
     * Check that a composable template does create an alias for a system index because of allows templates
     */
    public void testCreateSystemAliasViaComposableTemplateWithAllowsTemplates() throws Exception {
        createIndexWithComposableTemplates(
            TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME,
            TestSystemIndexDescriptorAllowsTemplates.PRIMARY_INDEX_NAME
        );

        assertHasAliases(
            Set.of(
                TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME,
                TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME + "-composable-alias"
            ),
            TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME,
            TestSystemIndexDescriptorAllowsTemplates.PRIMARY_INDEX_NAME,
            2
        );

        assertAcked(
            client().execute(
                TransportDeleteComposableIndexTemplateAction.TYPE,
                new TransportDeleteComposableIndexTemplateAction.Request("test-composable-template")
            )
        );
    }

    private void doCreateTest(Runnable runnable, String concreteIndex) {
        // Trigger the creation of the system index
        runnable.run();
        ensureGreen(INDEX_NAME);

        assertMappingsAndSettings(TestSystemIndexDescriptor.getOldMappings(), concreteIndex);

        // Remove the index and alias...
        assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).removeAlias(concreteIndex, INDEX_NAME).get());
        assertAcked(indicesAdmin().prepareDelete(concreteIndex));

        // ...so that we can check that the they will still be auto-created again,
        // but this time with updated settings
        TestSystemIndexDescriptor.useNewMappings.set(true);

        runnable.run();
        ensureGreen(INDEX_NAME);

        assertMappingsAndSettings(TestSystemIndexDescriptor.getNewMappings(), concreteIndex);
        assertAliases(concreteIndex);
    }

    public void testConcurrentAutoCreates() throws InterruptedException {
        final Client client = client();
        final int count = randomIntBetween(5, 30);
        final CountDownLatch latch = new CountDownLatch(count);
        final ActionListener<BulkResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse o) {
                latch.countDown();
                assertFalse(o.hasFailures());
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
                throw new AssertionError(e);
            }
        };
        for (int i = 0; i < count; i++) {
            client.bulk(new BulkRequest().add(new IndexRequest(INDEX_NAME).source(Map.of("foo", "bar"))), listener);
        }
        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    /**
     * Make sure that aliases are created hidden
     */
    private void assertAliases(String concreteIndex) {
        final GetAliasesResponse getAliasesResponse = indicesAdmin().getAliases(
            new GetAliasesRequest(TEST_REQUEST_TIMEOUT).indicesOptions(IndicesOptions.strictExpandHidden())
        ).actionGet();

        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get(concreteIndex).size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get(concreteIndex).get(0).isHidden(), equalTo(true));
        assertThat(getAliasesResponse.getAliases().get(concreteIndex).get(0).writeIndex(), equalTo(true));
    }

    private void assertHasAliases(Set<String> aliasNames, String name, String primaryName, int aliasCount) throws InterruptedException,
        java.util.concurrent.ExecutionException {
        final GetAliasesResponse getAliasesResponse = indicesAdmin().getAliases(
            new GetAliasesRequest(TEST_REQUEST_TIMEOUT).indicesOptions(IndicesOptions.strictExpandHidden())
        ).get();

        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get(primaryName).size(), equalTo(aliasCount));
        assertThat(
            getAliasesResponse.getAliases().get(primaryName).stream().map(AliasMetadata::alias).collect(Collectors.toSet()),
            equalTo(aliasNames)
        );
        for (AliasMetadata aliasMetadata : getAliasesResponse.getAliases().get(primaryName)) {
            assertThat(aliasMetadata.isHidden(), equalTo(true));
            if (aliasMetadata.alias().equals(name)) {
                assertThat(aliasMetadata.writeIndex(), is(true));
            } else {
                assertThat(aliasMetadata.writeIndex(), is(nullValue()));
            }
        }
    }

    /**
     * Fetch the mappings and settings for {@link TestSystemIndexDescriptor#INDEX_NAME} and verify that they match the expected values.
     */
    private void assertMappingsAndSettings(String expectedMappings, String concreteIndex) {
        final GetMappingsResponse getMappingsResponse = indicesAdmin().getMappings(
            new GetMappingsRequest(TEST_REQUEST_TIMEOUT).indices(INDEX_NAME)
        ).actionGet();

        final Map<String, MappingMetadata> mappings = getMappingsResponse.getMappings();
        assertThat(
            "Expected mappings to contain a key for [" + concreteIndex + "], but found: " + mappings.toString(),
            mappings.containsKey(concreteIndex),
            equalTo(true)
        );
        final Map<String, Object> sourceAsMap = mappings.get(concreteIndex).getSourceAsMap();

        assertThat(sourceAsMap, equalTo(XContentHelper.convertToMap(XContentType.JSON.xContent(), expectedMappings, false)));

        final GetSettingsResponse getSettingsResponse = indicesAdmin().getSettings(
            new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(INDEX_NAME)
        ).actionGet();

        final Settings actual = getSettingsResponse.getIndexToSettings().get(concreteIndex);

        for (String settingName : TestSystemIndexDescriptor.SETTINGS.keySet()) {
            assertThat(actual.get(settingName), equalTo(TestSystemIndexDescriptor.SETTINGS.get(settingName)));
        }
    }

}
