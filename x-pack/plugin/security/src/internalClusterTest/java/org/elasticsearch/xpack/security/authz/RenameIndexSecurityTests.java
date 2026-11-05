/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
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
import static org.hamcrest.Matchers.containsString;
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
            + "\n"
            + "esc_secret:"
            + hashed
            + "\n"
            + "esc_ds:"
            + hashed
            + "\n"
            + "esc_dls:"
            + hashed
            + "\n"
            + "esc_fls:"
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
            + "ds_only:ds_only\n"
            + "esc_secret:esc_secret\n"
            + "esc_ds:esc_ds\n"
            + "esc_dls:esc_dls\n"
            + "esc_fls:esc_fls\n";
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
            esc_secret:
              indices:
                - names: 'secret-*'
                  privileges: [ manage ]
                - names: 'public-*'
                  privileges: [ read, manage ]
            esc_ds:
              indices:
                - names: 'logs'
                  privileges: [ manage ]
                - names: 'public-*'
                  privileges: [ read, manage ]
            esc_dls:
              indices:
                - names: 'dlsr-src*'
                  privileges: [ read, manage ]
                  query: '{"term": {"visible": true}}'
                - names: 'dlsr-dst*'
                  privileges: [ read, manage ]
            esc_fls:
              indices:
                - names: 'flsr-src*'
                  privileges: [ read, manage ]
                  field_security:
                    grant: [ visible ]
                - names: 'flsr-dst*'
                  privileges: [ read, manage ]
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

    /**
     * A shard id captured before a rename must not keep working under its old name for a user who is authorized for that name but
     * not for the index's new one. The request is authorized for the name it carries, so it must be routed by that name, and since
     * no index has it any more the request fails instead of following the UUID to the renamed index.
     */
    public void testStaleShardNameDoesNotRetainPrivilegesAfterRename() {
        createIndex("old-lease", 1, 0, false);
        ensureGreen("old-lease");
        final var oldIndex = clusterService().state().metadata().getProject().index("old-lease").getIndex();
        final var oldShard = new ShardId(oldIndex, 0);
        client().execute(RetentionLeaseActions.ADD, new RetentionLeaseActions.AddRequest(oldShard, "review-lease", 0, "review"))
            .actionGet();
        rename(client(), "old-lease", "new-lease");
        final var newIndex = clusterService().state().metadata().getProject().index("new-lease").getIndex();
        final var newShard = new ShardId(newIndex, 0);
        final Client restricted = clientAs("rename_source_only");
        assertThrowsAuthorizationException(
            () -> restricted.execute(
                RetentionLeaseActions.RENEW,
                new RetentionLeaseActions.RenewRequest(newShard, "review-lease", 0, "review")
            ).actionGet(),
            RetentionLeaseActions.RENEW.name(),
            "rename_source_only"
        );
        expectThrows(
            IndexNotFoundException.class,
            () -> restricted.execute(
                RetentionLeaseActions.RENEW,
                new RetentionLeaseActions.RenewRequest(oldShard, "review-lease", 0, "review")
            ).actionGet()
        );
    }

    /**
     * A shard id that pairs a name the user may access with the UUID of an index they may not must not reach that index, with no
     * rename involved: the request is authorized for the name, so the name decides where it goes.
     */
    public void testForgedShardNameCannotRemoveAnotherIndicesRetentionLease() {
        createIndex("old-public", 1, 0, false);
        createIndex("new-private", 1, 0, false);
        ensureGreen("old-public", "new-private");
        final var privateIndex = clusterService().state().metadata().getProject().index("new-private").getIndex();
        final var privateShard = new ShardId(privateIndex, 0);
        client().execute(RetentionLeaseActions.ADD, new RetentionLeaseActions.AddRequest(privateShard, "private-lease", 0, "review"))
            .actionGet();
        final var forgedShard = new ShardId(new Index("old-public", privateIndex.getUUID()), 0);
        final Client restricted = clientAs("rename_source_only");
        assertThrowsAuthorizationException(
            () -> restricted.execute(RetentionLeaseActions.REMOVE, new RetentionLeaseActions.RemoveRequest(privateShard, "private-lease"))
                .actionGet(),
            RetentionLeaseActions.REMOVE.name(),
            "rename_source_only"
        );
        expectThrows(
            IndexNotFoundException.class,
            () -> restricted.execute(RetentionLeaseActions.REMOVE, new RetentionLeaseActions.RemoveRequest(forgedShard, "private-lease"))
                .actionGet()
        );
        // and the lease is still there
        final var leases = indicesAdmin().prepareStats("new-private").get().getShards()[0].getRetentionLeaseStats().retentionLeases();
        assertTrue(leases.contains("private-lease"));
    }

    /**
     * A rename carries the data of an index to another name, so the new name must not give more access than the old one does. A user
     * who may only manage {@code secret-*} must not be able to move an index under a name they can read.
     */
    public void testCannotRenameAnIndexToANameWithMorePermissions() {
        createIndex("secret-1", 1, 0, false);
        indexDoc("secret-1", "1", "field", "value");
        refresh("secret-1");

        assertRefusedForMorePermissions(() -> rename(clientAs("esc_secret"), "secret-1", "public-secret-1"));
        assertTrue(indexExists("secret-1"));
        assertFalse(indexExists("public-secret-1"));
    }

    /** Authorized through the data stream name, a backing index has the permissions of the data stream, not of its own name. */
    public void testCannotRenameABackingIndexToANameWithMorePermissions() throws Exception {
        createDataStream("logs");
        final String backingIndex = backingIndices("logs").get(0).getName();

        assertRefusedForMorePermissions(() -> rename(clientAs("esc_ds"), backingIndex, "public-logs-1"));
        assertTrue(indexExists(backingIndex));
        assertFalse(indexExists("public-logs-1"));
    }

    /** As for adding an alias, a user whose access to the index is restricted by document level security may not rename it. */
    public void testCannotRenameAnIndexWithDocumentLevelSecurity() {
        createIndex("dlsr-src-1", 1, 0, false);
        indexDoc("dlsr-src-1", "1", "visible", false);
        refresh("dlsr-src-1");

        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> rename(clientAs("esc_dls"), "dlsr-src-1", "dlsr-dst-1")
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("field or document level security"));
        assertTrue(indexExists("dlsr-src-1"));
        assertFalse(indexExists("dlsr-dst-1"));
    }

    /** As for adding an alias, a user whose access to the index is restricted by field level security may not rename it. */
    public void testCannotRenameAnIndexWithFieldLevelSecurity() {
        createIndex("flsr-src-1", 1, 0, false);
        indexDoc("flsr-src-1", "1", "visible", "v", "secret", "s");
        refresh("flsr-src-1");

        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> rename(clientAs("esc_fls"), "flsr-src-1", "flsr-dst-1")
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("field or document level security"));
        assertTrue(indexExists("flsr-src-1"));
        assertFalse(indexExists("flsr-dst-1"));
    }

    private static void assertRefusedForMorePermissions(org.elasticsearch.core.CheckedRunnable<Exception> rename) {
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, rename::run);
        assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), containsString("the new name has more permissions than the index"));
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
