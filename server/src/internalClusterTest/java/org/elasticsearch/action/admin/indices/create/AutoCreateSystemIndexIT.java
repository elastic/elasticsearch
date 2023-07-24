/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.indices.TestSystemIndexDescriptorAllowsTemplates;
import org.elasticsearch.indices.TestSystemIndexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.snapshots.SystemIndicesSnapshotIT;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_HIDDEN;
import static org.elasticsearch.indices.TestSystemIndexDescriptor.INDEX_NAME;
import static org.elasticsearch.indices.TestSystemIndexDescriptor.PRIMARY_INDEX_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AutoCreateSystemIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), TestSystemIndexPlugin.class),
            UnmanagedSystemIndexTestPlugin.class
        );
    }

    @After
    public void afterEach() {
        assertAcked(indicesAdmin().prepareDeleteTemplate("*").get());
        indicesAdmin().prepareDelete(PRIMARY_INDEX_NAME);
    }

    public void testAutoCreatePrimaryIndex() throws Exception {
        CreateIndexRequest request = new CreateIndexRequest(PRIMARY_INDEX_NAME);
        client().execute(AutoCreateAction.INSTANCE, request).get();

        GetIndexResponse response = indicesAdmin().prepareGetIndex().addIndices(PRIMARY_INDEX_NAME).get();
        assertThat(response.indices().length, is(1));
        assertThat(response.aliases().size(), is(1));
        assertThat(response.aliases().get(PRIMARY_INDEX_NAME).size(), is(1));
        assertThat(
            response.aliases().get(PRIMARY_INDEX_NAME).get(0),
            equalTo(AliasMetadata.builder(INDEX_NAME).isHidden(true).writeIndex(true).build())
        );
    }

    public void testAutoCreatePrimaryIndexFromAlias() throws Exception {
        CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);
        client().execute(AutoCreateAction.INSTANCE, request).get();

        GetIndexResponse response = indicesAdmin().prepareGetIndex().addIndices(PRIMARY_INDEX_NAME).get();
        assertThat(response.indices().length, is(1));
        assertThat(response.aliases().size(), is(1));
        assertThat(response.aliases().get(PRIMARY_INDEX_NAME).size(), is(1));
        assertThat(
            response.aliases().get(PRIMARY_INDEX_NAME).get(0),
            equalTo(AliasMetadata.builder(INDEX_NAME).isHidden(true).writeIndex(true).build())
        );
    }

    public void testAutoCreateNonPrimaryIndex() throws Exception {
        CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME + "-2");
        client().execute(AutoCreateAction.INSTANCE, request).get();

        GetIndexResponse response = indicesAdmin().prepareGetIndex().addIndices(INDEX_NAME + "-2").get();
        assertThat(response.indices().length, is(1));
        assertThat(response.aliases().size(), is(1));
        assertThat(response.aliases().get(INDEX_NAME + "-2").size(), is(1));
        assertThat(response.aliases().get(INDEX_NAME + "-2").get(0), equalTo(AliasMetadata.builder(INDEX_NAME).isHidden(true).build()));
    }

    public void testWriteToAliasPrimaryAutoCreatedFirst() throws Exception {
        {
            CreateIndexRequest request = new CreateIndexRequest(PRIMARY_INDEX_NAME);
            client().execute(AutoCreateAction.INSTANCE, request).get();
        }

        {
            CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME + "-2");
            client().execute(AutoCreateAction.INSTANCE, request).get();
        }

        IndexResponse response = client().prepareIndex(INDEX_NAME).setSource("{\"foo\":\"bar\"}", XContentType.JSON).get();
        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));
    }

    /**
     * Like {@link #testWriteToAliasPrimaryAutoCreatedFirst()}, but with indices created in the opposite order
     * @throws Exception
     */
    public void testWriteToAliasSecondaryAutoCreatedFirst() throws Exception {
        {
            CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME + "-2");
            client().execute(AutoCreateAction.INSTANCE, request).get();
        }

        {
            CreateIndexRequest request = new CreateIndexRequest(PRIMARY_INDEX_NAME);
            client().execute(AutoCreateAction.INSTANCE, request).get();
        }

        IndexResponse response = client().prepareIndex(INDEX_NAME).setSource("{\"foo\":\"bar\"}", XContentType.JSON).get();
        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));
    }

    public void testSystemIndicesAutoCreatedAsHidden() throws Exception {
        CreateIndexRequest request = new CreateIndexRequest(UnmanagedSystemIndexTestPlugin.SYSTEM_INDEX_NAME);
        client().execute(AutoCreateAction.INSTANCE, request).get();

        GetIndexResponse response = indicesAdmin().prepareGetIndex().addIndices(UnmanagedSystemIndexTestPlugin.SYSTEM_INDEX_NAME).get();
        assertThat(response.indices().length, is(1));
        Settings settings = response.settings().get(UnmanagedSystemIndexTestPlugin.SYSTEM_INDEX_NAME);
        assertThat(settings, notNullValue());
        assertThat(settings.getAsBoolean(SETTING_INDEX_HIDDEN, false), is(true));
    }

    public void testSystemIndicesAutoCreateRejectedWhenNotHidden() {
        CreateIndexRequest request = new CreateIndexRequest(UnmanagedSystemIndexTestPlugin.SYSTEM_INDEX_NAME);
        request.settings(Settings.builder().put(SETTING_INDEX_HIDDEN, false).build());
        ExecutionException exception = expectThrows(
            ExecutionException.class,
            () -> client().execute(AutoCreateAction.INSTANCE, request).get()
        );

        assertThat(
            exception.getCause().getMessage(),
            containsString("Cannot auto-create system index [.unmanaged-system-idx] with [index.hidden] set to 'false'")
        );
    }

    private String autoCreateSystemAliasViaV1Template(String indexName) throws Exception {
        assertAcked(
            indicesAdmin().preparePutTemplate("test-template")
                .setPatterns(List.of(indexName + "*"))
                .addAlias(new Alias(indexName + "-legacy-alias"))
                .get()
        );

        String nonPrimaryIndex = indexName + "-2";
        CreateIndexRequest request = new CreateIndexRequest(nonPrimaryIndex);
        assertAcked(client().execute(AutoCreateAction.INSTANCE, request).get());
        assertTrue(indexExists(nonPrimaryIndex));

        return nonPrimaryIndex;
    }

    /**
     * Check that a legacy template does not create an alias for a system index
     */
    public void testAutoCreateSystemAliasViaV1Template() throws Exception {
        var nonPrimaryIndex = autoCreateSystemAliasViaV1Template(INDEX_NAME);

        assertAliasesHidden(nonPrimaryIndex, Set.of(INDEX_NAME), 1);
    }

    /**
     * Check that a legacy template does create an alias for a system index, because of allows templates
     */
    public void testAutoCreateSystemAliasViaV1TemplateAllowsTemplates() throws Exception {
        var nonPrimaryIndex = autoCreateSystemAliasViaV1Template(TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME);

        assertAliasesHidden(
            nonPrimaryIndex,
            Set.of(
                TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME,
                TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME + "-legacy-alias"
            ),
            2
        );
    }

    private String autoCreateSystemAliasViaComposableTemplate(String indexName) throws Exception {
        ComposableIndexTemplate cit = new ComposableIndexTemplate(
            Collections.singletonList(indexName + "*"),
            new Template(
                null,
                null,
                Map.of(indexName + "-composable-alias", AliasMetadata.builder(indexName + "-composable-alias").build())
            ),
            Collections.emptyList(),
            4L,
            5L,
            Collections.emptyMap()
        );
        assertAcked(
            client().execute(
                PutComposableIndexTemplateAction.INSTANCE,
                new PutComposableIndexTemplateAction.Request("test-composable-template").indexTemplate(cit)
            ).get()
        );

        String nonPrimaryIndex = indexName + "-2";
        CreateIndexRequest request = new CreateIndexRequest(nonPrimaryIndex);
        assertAcked(client().execute(AutoCreateAction.INSTANCE, request).get());

        assertTrue(indexExists(nonPrimaryIndex));

        return nonPrimaryIndex;
    }

    /**
     * Check that a composable template does not create an alias for a system index
     */
    public void testAutoCreateSystemAliasViaComposableTemplate() throws Exception {
        String nonPrimaryIndex = autoCreateSystemAliasViaComposableTemplate(INDEX_NAME);

        assertAliasesHidden(nonPrimaryIndex, Set.of(INDEX_NAME), 1);

        assertAcked(
            client().execute(
                DeleteComposableIndexTemplateAction.INSTANCE,
                new DeleteComposableIndexTemplateAction.Request("test-composable-template")
            ).get()
        );
    }

    /**
     * Check that a composable template does create an alias for a system index, because of allows templates
     */
    public void testAutoCreateSystemAliasViaComposableTemplateAllowsTemplates() throws Exception {
        String nonPrimaryIndex = autoCreateSystemAliasViaComposableTemplate(TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME);

        assertAliasesHidden(
            nonPrimaryIndex,
            Set.of(
                TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME,
                TestSystemIndexDescriptorAllowsTemplates.INDEX_NAME + "-composable-alias"
            ),
            2
        );

        assertAcked(
            client().execute(
                DeleteComposableIndexTemplateAction.INSTANCE,
                new DeleteComposableIndexTemplateAction.Request("test-composable-template")
            ).get()
        );
    }

    private void assertAliasesHidden(String nonPrimaryIndex, Set<String> aliasNames, int aliasCount) throws InterruptedException,
        ExecutionException {
        final GetAliasesResponse getAliasesResponse = indicesAdmin().getAliases(
            new GetAliasesRequest().indicesOptions(IndicesOptions.strictExpandHidden())
        ).get();

        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get(nonPrimaryIndex).size(), equalTo(aliasCount));
        assertThat(
            getAliasesResponse.getAliases().get(nonPrimaryIndex).stream().map(AliasMetadata::alias).collect(Collectors.toSet()),
            equalTo(aliasNames)
        );
        getAliasesResponse.getAliases().get(nonPrimaryIndex).forEach(alias -> assertThat(alias.isHidden(), is(true)));
    }

    public static class UnmanagedSystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final String SYSTEM_INDEX_NAME = ".unmanaged-system-idx";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(
                SystemIndexDescriptorUtils.createUnmanaged(SYSTEM_INDEX_NAME + "*", "System indices for tests")
            );
        }

        @Override
        public String getFeatureName() {
            return SystemIndicesSnapshotIT.SystemIndexTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A simple test plugin";
        }
    }

}
