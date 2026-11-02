/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationException;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

/**
 * The privilege model of index renames: the caller needs {@code manage} on the index's current name and on its new name. For a
 * data stream backing index the current name may be authorized through the data stream's name, as any backing index privilege
 * is, while the new name, which does not exist yet, is authorized by role pattern alone.
 */
public class RenameIndexSecurityTests extends SecurityIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    @After
    public void cleanupSecurityIndex() {
        super.deleteSecurityIndex();
    }

    @Override
    protected String configUsers() {
        final String hashed = SecuritySettingsSource.TEST_PASSWORD_HASHED;
        return super.configUsers()
            + "rename_both:"
            + hashed
            + "\n"
            + "rename_source_only:"
            + hashed
            + "\n"
            + "rename_none:"
            + hashed
            + "\n"
            + "ds_manage:"
            + hashed
            + "\n"
            + "ds_only:"
            + hashed
            + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles()
            + "rename_both:rename_both\n"
            + "rename_source_only:rename_source_only\n"
            + "rename_none:rename_none\n"
            + "ds_manage:ds_manage\n"
            + "ds_only:ds_only\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + """

            rename_both:
              indices:
                - names: [ 'old*', 'new*' ]
                  privileges: [ manage ]
            rename_source_only:
              indices:
                - names: 'old*'
                  privileges: [ manage ]
            rename_none:
              indices:
                - names: 'other*'
                  privileges: [ manage ]
            ds_manage:
              indices:
                - names: [ 'logs', 'new*' ]
                  privileges: [ manage ]
            ds_only:
              indices:
                - names: 'logs'
                  privileges: [ manage ]
            """;
    }

    public void testManageOnBothNamesCanRename() {
        createIndex("old-1", 1, 0, false);
        rename(clientAs("rename_both"), "old-1", "new-1");
        assertFalse(indexExists("old-1"));
        assertTrue(indexExists("new-1"));
    }

    public void testManageOnSourceOnlyIsRefused() {
        createIndex("old-2", 1, 0, false);
        assertThrowsAuthorizationException(
            () -> rename(clientAs("rename_source_only"), "old-2", "new-2"),
            TransportIndicesAliasesAction.NAME,
            "rename_source_only"
        );
        assertTrue(indexExists("old-2"));
        assertFalse(indexExists("new-2"));
    }

    public void testNoPrivilegeOnEitherNameIsRefused() {
        createIndex("old-3", 1, 0, false);
        assertThrowsAuthorizationException(
            () -> rename(clientAs("rename_none"), "old-3", "new-3"),
            TransportIndicesAliasesAction.NAME,
            "rename_none"
        );
        assertTrue(indexExists("old-3"));
        assertFalse(indexExists("new-3"));
    }

    public void testBackingIndexAuthorizedThroughDataStreamName() throws Exception {
        createDataStream("logs");
        final String backingIndex = backingIndices("logs").get(0).getName();

        // manage on the data stream covers its backing index; the new name must be authorized on its own
        assertThrowsAuthorizationException(
            () -> rename(clientAs("ds_only"), backingIndex, "new-ds-1"),
            TransportIndicesAliasesAction.NAME,
            "ds_only"
        );
        assertTrue(indexExists(backingIndex));

        rename(clientAs("ds_manage"), backingIndex, "new-ds-1");
        assertFalse(indexExists(backingIndex));
        assertTrue(indexExists("new-ds-1"));
        // the renamed index is still the data stream's backing index, so access through the data stream name continues
        assertThat(backingIndices("logs").get(0).getName(), equalTo("new-ds-1"));
    }

    private Client clientAs(String user) {
        return client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING))
        );
    }

    private static void rename(Client client, String source, String target) {
        final var response = client.execute(TransportIndicesAliasesAction.TYPE, renameIndexRequest(source, target)).actionGet();
        assertTrue(response.isAcknowledged());
    }

    private void createDataStream(String name) throws Exception {
        final TransportPutComposableIndexTemplateAction.Request template = new TransportPutComposableIndexTemplateAction.Request("logs");
        template.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("logs*"))
                // the test cluster may have a single data node, so the backing index must not need replicas
                .template(Template.builder().settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, template).get());
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name)
            ).get()
        );
        ensureGreen(name);
    }

    private List<org.elasticsearch.index.Index> backingIndices(String dataStream) {
        final GetDataStreamAction.Response response = client().execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStream })
        ).actionGet();
        assertThat(response.getDataStreams().size(), equalTo(1));
        final DataStream ds = response.getDataStreams().get(0).getDataStream();
        return ds.getIndices();
    }
}
