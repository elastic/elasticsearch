/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.LicenseInfo;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.transform.transforms.SourceDestValidator.Context;
import org.elasticsearch.xpack.transform.transforms.SourceDestValidator.RemoteSourceEnabledAndRemoteLicenseValidation;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.spy;

public class SourceDestValidatorTests extends ESTestCase {

    private static final String SOURCE_1 = "source-1";
    private static final String SOURCE_1_ALIAS = "source-1-alias";
    private static final String SOURCE_2 = "source-2";
    private static final String DEST_ALIAS = "dest-alias";
    private static final String ALIASED_DEST = "aliased-dest";
    private static final String ALIAS_READ_WRITE_DEST = "alias-read-write-dest";
    private static final String REMOTE_BASIC = "remote-basic";
    private static final String REMOTE_PLATINUM = "remote-platinum";

    private static final ClusterState CLUSTER_STATE;
    private Client clientWithBasicLicense;
    private Client clientWithPlatinumLicense;
    private RemoteClusterLicenseChecker remoteClusterLicenseCheckerBasic;

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final TransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool);
    private final RemoteClusterService remoteClusterService = transportService.getRemoteClusterService();

    static {
        IndexMetaData source1 = IndexMetaData.builder(SOURCE_1)
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            )
            .putAlias(AliasMetaData.builder(SOURCE_1_ALIAS).build())
            .putAlias(AliasMetaData.builder(ALIAS_READ_WRITE_DEST).writeIndex(false).build())
            .build();
        IndexMetaData source2 = IndexMetaData.builder(SOURCE_2)
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            )
            .putAlias(AliasMetaData.builder(DEST_ALIAS).build())
            .putAlias(AliasMetaData.builder(ALIAS_READ_WRITE_DEST).writeIndex(false).build())
            .build();
        IndexMetaData aliasedDest = IndexMetaData.builder(ALIASED_DEST)
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            )
            .putAlias(AliasMetaData.builder(DEST_ALIAS).build())
            .putAlias(AliasMetaData.builder(ALIAS_READ_WRITE_DEST).build())
            .build();
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.metaData(
            MetaData.builder()
                .put(IndexMetaData.builder(source1).build(), false)
                .put(IndexMetaData.builder(source2).build(), false)
                .put(IndexMetaData.builder(aliasedDest).build(), false)
        );
        CLUSTER_STATE = state.build();
    }

    private class MockClientLicenseCheck extends NoOpClient {
        private final String license;

        MockClientLicenseCheck(String testName, String license) {
            super(testName);
            this.license = license;
        }

        @Override
        public Client getRemoteClusterClient(String clusterAlias) {
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof XPackInfoRequest) {
                XPackInfoResponse response = new XPackInfoResponse(
                    null,
                    new LicenseInfo("uid", license, license, LicenseStatus.ACTIVE, randomNonNegativeLong()),
                    null
                );
                listener.onResponse((Response) response);
                return;
            }
            super.doExecute(action, request, listener);
        }
    }

    @Before
    public void setupComponents() {
        clientWithBasicLicense = new MockClientLicenseCheck(getTestName(), "basic");
        remoteClusterLicenseCheckerBasic = new RemoteClusterLicenseChecker(
            clientWithBasicLicense,
            (operationMode -> operationMode != License.OperationMode.MISSING)
        );
        clientWithPlatinumLicense = new MockClientLicenseCheck(getTestName(), "platinum");
    }

    @After
    public void closeComponents() throws Exception {
        clientWithBasicLicense.close();
        clientWithPlatinumLicense.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testCheck_GivenSimpleSourceIndexAndValidDestIndex() {
        ValidationException e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { SOURCE_1 },
            "dest",
            "node_id",
            "license",
            false
        );
        assertNull(e);
    }

    public void testCheck_GivenNoSourceIndexAndValidDestIndex() {
        ValidationException e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] {},
            "dest",
            "node_id",
            "license",
            false
        );

        assertNotNull(e);
        assertEquals(1, e.validationErrors().size());
        assertThat(e.validationErrors().get(0), equalTo("Source index [] does not exist"));
    }

    public void testCheck_GivenMissingConcreteSourceIndex() {
        ValidationException e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { "missing" },
            "dest",
            "node_id",
            "license",
            false
        );

        assertNotNull(e);
        assertEquals(1, e.validationErrors().size());
        assertThat(e.validationErrors().get(0), equalTo("no such index [missing]"));
        e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { "missing" },
            "dest",
            "node_id",
            "license",
            true
        );
        assertNull(e);
    }

    public void testCheck_GivenMixedMissingAndExistingConcreteSourceIndex() {
        ValidationException e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { SOURCE_1, "missing" },
            "dest",
            "node_id",
            "license",
            false
        );

        assertNotNull(e);
        assertEquals(1, e.validationErrors().size());
        assertThat(e.validationErrors().get(0), equalTo("no such index [missing]"));
        e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { SOURCE_1, "missing" },
            "dest",
            "node_id",
            "license",
            true
        );
        assertNull(e);
    }

    public void testCheck_GivenMixedMissingWildcardExistingConcreteSourceIndex() {
        ValidationException e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { SOURCE_1, "wildcard*", "missing" },
            "dest",
            "node_id",
            "license",
            false
        );

        assertNotNull(e);
        assertEquals(1, e.validationErrors().size());
        assertThat(e.validationErrors().get(0), equalTo("no such index [missing]"));
        e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { SOURCE_1, "wildcard*", "missing" },
            "dest",
            "node_id",
            "license",
            true
        );
        assertNull(e);
    }

    public void testCheck_GivenWildcardSourceIndex() {
        ValidationException e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { "wildcard*" },
            "dest",
            "node_id",
            "license",
            false
        );
        assertNull(e);
    }

    public void testCheck_GivenDestIndexSameAsSourceIndex() {
        ValidationException e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { SOURCE_1 },
            "source-1",
            "node_id",
            "license",
            false
        );

        assertNotNull(e);
        assertEquals(1, e.validationErrors().size());
        assertThat(
            e.validationErrors().get(0),
            equalTo("Destination index [" + SOURCE_1 + "] is included in source expression [" + SOURCE_1 + "]")
        );
        e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { SOURCE_1 },
            "source-1",
            "node_id",
            "license",
            true
        );
        assertNull(e);
    }

    public void testCheck_GivenDestIndexMatchesSourceIndex() {
        ValidationException e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { "source-*" },
            SOURCE_2,
            "node_id",
            "license",
            false
        );
        assertNotNull(e);
        assertEquals(1, e.validationErrors().size());
        assertThat(
            e.validationErrors().get(0),
            equalTo("Destination index [" + SOURCE_2 + "] is included in source expression [source-*]")
        );
        e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { "source-*" },
            SOURCE_2,
            "node_id",
            "license",
            true
        );
        assertNull(e);
    }

    public void testCheck_GivenDestIndexMatchesOneOfSourceIndices() {
        ValidationException e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { "source-1", "source-*" },
            SOURCE_2,
            "node_id",
            "license",
            false
        );
        assertNotNull(e);
        assertEquals(1, e.validationErrors().size());
        assertThat(
            e.validationErrors().get(0),
            equalTo("Destination index [" + SOURCE_2 + "] is included in source expression [source-*]")
        );
        e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { "source-1", "source-*" },
            SOURCE_2,
            "node_id",
            "license",
            true
        );
        assertNull(e);
    }

    public void testCheck_GivenDestIndexIsAliasThatMatchesMultipleIndices() {
        ValidationException e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { SOURCE_1 },
            DEST_ALIAS,
            "node_id",
            "license",
            false
        );
        assertNotNull(e);
        assertEquals(1, e.validationErrors().size());
        assertThat(
            e.validationErrors().get(0),
            equalTo(
                "no write index is defined for alias [dest-alias]. "
                    + "The write index may be explicitly disabled using is_write_index=false or the alias points "
                    + "to multiple indices without one being designated as a write index"
            )
        );

        e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { SOURCE_1 },
            DEST_ALIAS,
            "node_id",
            "license",
            true
        );
        assertNotNull(e);
        assertEquals(1, e.validationErrors().size());
        assertThat(
            e.validationErrors().get(0),
            equalTo(
                "no write index is defined for alias [dest-alias]. "
                    + "The write index may be explicitly disabled using is_write_index=false or the alias points "
                    + "to multiple indices without one being designated as a write index"
            )
        );
    }

    public void testCheck_GivenDestIndexIsAliasThatMatchesMultipleIndicesButHasSingleWriteAlias() {
        ValidationException e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { SOURCE_1 },
            ALIAS_READ_WRITE_DEST,
            "node_id",
            "license",
            false
        );
        assertNotNull(e);
    }

    public void testCheck_GivenDestIndexIsAliasThatIsIncludedInSource() {
        ValidationException e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { SOURCE_1 },
            SOURCE_1_ALIAS,
            "node_id",
            "license",
            false
        );
        assertNotNull(e);
        assertEquals(1, e.validationErrors().size());
        assertThat(
            e.validationErrors().get(0),
            equalTo("Destination index [" + SOURCE_1 + "] is included in source expression [" + SOURCE_1 + "]")
        );

        e = SourceDestValidator.validate(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            null,
            new String[] { SOURCE_1 },
            SOURCE_1_ALIAS,
            "node_id",
            "license",
            true
        );
        assertNull(e);
    }

    public void testRemoteSourceBasic() {

        Context context = new SourceDestValidator.Context(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            remoteClusterLicenseCheckerBasic,
            new String[] { REMOTE_BASIC + ":" + "SOURCE_1" },
            "dest",
            "node_id",
            "license"
        );

        Context spyContext = spy(context);
        when(spyContext.getRegisteredRemoteClusterNames()).thenReturn(Collections.singleton(REMOTE_BASIC));

        RemoteSourceEnabledAndRemoteLicenseValidation validator = new RemoteSourceEnabledAndRemoteLicenseValidation();
        validator.validate(spyContext);

        assertNull(context.getValidationException());
    }

    public void testRemoteSourcePlatinum() {

        Context context = new SourceDestValidator.Context(
            CLUSTER_STATE,
            new IndexNameExpressionResolver(),
            remoteClusterService,
            new RemoteClusterLicenseChecker(clientWithBasicLicense, XPackLicenseState::isPlatinumOrTrialOperationMode),
            new String[] { REMOTE_BASIC + ":" + "SOURCE_1" },
            "dest",
            "node_id",
            "license"
        );

        Context spyContext = spy(context);
        when(spyContext.getRegisteredRemoteClusterNames()).thenReturn(Collections.singleton(REMOTE_BASIC));

        RemoteSourceEnabledAndRemoteLicenseValidation validator = new RemoteSourceEnabledAndRemoteLicenseValidation();
        validator.validate(spyContext);

        assertNotNull(spyContext.getValidationException());
    }
}
