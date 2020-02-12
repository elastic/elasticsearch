/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.common.validation;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.CheckedConsumer;
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
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.Context;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.RemoteSourceEnabledAndRemoteLicenseValidation;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.DESTINATION_IN_SOURCE_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.DESTINATION_SINGLE_INDEX_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.REMOTE_SOURCE_VALIDATION;
import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.SOURCE_MISSING_VALIDATION;
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

    private static final List<SourceDestValidator.SourceDestValidation> TEST_VALIDATIONS = Arrays.asList(
        SOURCE_MISSING_VALIDATION,
        DESTINATION_IN_SOURCE_VALIDATION,
        DESTINATION_SINGLE_INDEX_VALIDATION,
        REMOTE_SOURCE_VALIDATION
    );

    private Client clientWithBasicLicense;
    private Client clientWithExpiredBasicLicense;
    private Client clientWithPlatinumLicense;
    private Client clientWithTrialLicense;
    private RemoteClusterLicenseChecker remoteClusterLicenseCheckerBasic;

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final TransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool);
    private final RemoteClusterService remoteClusterService = transportService.getRemoteClusterService();
    private final SourceDestValidator simpleNonRemoteValidator = new SourceDestValidator(
        new IndexNameExpressionResolver(),
        remoteClusterService,
        null,
        "node_id",
        "license"
    );

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
        private final LicenseStatus licenseStatus;

        MockClientLicenseCheck(String testName, String license, LicenseStatus licenseStatus) {
            super(testName);
            this.license = license;
            this.licenseStatus = licenseStatus;
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
                    new LicenseInfo("uid", license, license, licenseStatus, randomNonNegativeLong()),
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
        clientWithBasicLicense = new MockClientLicenseCheck(getTestName(), "basic", LicenseStatus.ACTIVE);
        clientWithExpiredBasicLicense = new MockClientLicenseCheck(getTestName(), "basic", LicenseStatus.EXPIRED);
        remoteClusterLicenseCheckerBasic = new RemoteClusterLicenseChecker(
            clientWithBasicLicense,
            (operationMode -> operationMode != License.OperationMode.MISSING)
        );
        clientWithPlatinumLicense = new MockClientLicenseCheck(getTestName(), "platinum", LicenseStatus.ACTIVE);
        clientWithTrialLicense = new MockClientLicenseCheck(getTestName(), "trial", LicenseStatus.ACTIVE);
    }

    @After
    public void closeComponents() throws Exception {
        clientWithBasicLicense.close();
        clientWithExpiredBasicLicense.close();
        clientWithPlatinumLicense.close();
        clientWithTrialLicense.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testValidate_GivenSimpleSourceIndexAndValidDestIndex() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { SOURCE_1 },
                "dest",
                TEST_VALIDATIONS,
                listener
            ),
            true,
            null
        );
    }

    public void testCheck_GivenNoSourceIndexAndValidDestIndex() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] {},
                "dest",
                TEST_VALIDATIONS,
                listener
            ),
            (Boolean) null,
            e -> {
                assertEquals(1, e.validationErrors().size());
                assertThat(e.validationErrors().get(0), equalTo("Source index [] does not exist"));
            }
        );
    }

    public void testCheck_GivenMissingConcreteSourceIndex() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { "missing" },
                "dest",
                TEST_VALIDATIONS,
                listener
            ),
            (Boolean) null,
            e -> {
                assertEquals(1, e.validationErrors().size());
                assertThat(e.validationErrors().get(0), equalTo("no such index [missing]"));
            }
        );

        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { "missing" },
                "dest",
                Collections.emptyList(),
                listener
            ),
            true,
            null
        );
    }

    public void testCheck_GivenMixedMissingAndExistingConcreteSourceIndex() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { SOURCE_1, "missing" },
                "dest",
                TEST_VALIDATIONS,
                listener
            ),
            (Boolean) null,
            e -> {
                assertEquals(1, e.validationErrors().size());
                assertThat(e.validationErrors().get(0), equalTo("no such index [missing]"));
            }
        );

        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { SOURCE_1, "missing" },
                "dest",
                Collections.emptyList(),
                listener
            ),
            true,
            null
        );
    }

    public void testCheck_GivenMixedMissingWildcardExistingConcreteSourceIndex() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { SOURCE_1, "wildcard*", "missing" },
                "dest",
                TEST_VALIDATIONS,
                listener
            ),
            (Boolean) null,
            e -> {
                assertEquals(1, e.validationErrors().size());
                assertThat(e.validationErrors().get(0), equalTo("no such index [missing]"));
            }
        );

        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { SOURCE_1, "wildcard*", "missing" },
                "dest",
                Collections.emptyList(),
                listener
            ),
            true,
            null
        );
    }

    public void testCheck_GivenWildcardSourceIndex() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { "wildcard*" },
                "dest",
                TEST_VALIDATIONS,
                listener
            ),
            true,
            null
        );
    }

    public void testCheck_GivenDestIndexSameAsSourceIndex() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { SOURCE_1 },
                SOURCE_1,
                TEST_VALIDATIONS,
                listener
            ),
            (Boolean) null,
            e -> {
                assertEquals(1, e.validationErrors().size());
                assertThat(
                    e.validationErrors().get(0),
                    equalTo("Destination index [" + SOURCE_1 + "] is included in source expression [" + SOURCE_1 + "]")
                );
            }
        );

        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { SOURCE_1 },
                SOURCE_1,
                Collections.emptyList(),
                listener
            ),
            true,
            null
        );
    }

    public void testCheck_GivenDestIndexMatchesSourceIndex() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { "source-*" },
                SOURCE_2,
                TEST_VALIDATIONS,
                listener
            ),
            (Boolean) null,
            e -> {
                assertEquals(1, e.validationErrors().size());
                assertThat(
                    e.validationErrors().get(0),
                    equalTo("Destination index [" + SOURCE_2 + "] is included in source expression [source-*]")
                );
            }
        );

        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { "source-*" },
                SOURCE_2,
                Collections.emptyList(),
                listener
            ),
            true,
            null
        );
    }

    public void testCheck_GivenDestIndexMatchesOneOfSourceIndices() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { "source-1", "source-*" },
                SOURCE_2,
                TEST_VALIDATIONS,
                listener
            ),
            (Boolean) null,
            e -> {
                assertEquals(1, e.validationErrors().size());
                assertThat(
                    e.validationErrors().get(0),
                    equalTo("Destination index [" + SOURCE_2 + "] is included in source expression [source-*]")
                );
            }
        );

        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { "source-1", "source-*" },
                SOURCE_2,
                Collections.emptyList(),
                listener
            ),
            true,
            null
        );
    }

    public void testCheck_GivenDestIndexMatchesMultipleSourceIndices() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { "source-1", "source-*", "sou*" },
                SOURCE_2,
                TEST_VALIDATIONS,
                listener
            ),
            (Boolean) null,
            e -> {
                assertEquals(2, e.validationErrors().size());
                assertThat(
                    e.validationErrors().get(0),
                    equalTo("Destination index [" + SOURCE_2 + "] is included in source expression [source-*]")
                );
                assertThat(
                    e.validationErrors().get(1),
                    equalTo("Destination index [" + SOURCE_2 + "] is included in source expression [sou*]")
                );
            }
        );
    }

    public void testCheck_GivenDestIndexIsAliasThatMatchesMultipleIndices() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { SOURCE_1 },
                DEST_ALIAS,
                TEST_VALIDATIONS,
                listener
            ),
            (Boolean) null,
            e -> {
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
        );

        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { SOURCE_1 },
                DEST_ALIAS,
                Collections.emptyList(),
                listener
            ),
            true,
            null
        );
    }

    public void testCheck_GivenDestIndexIsAliasThatMatchesMultipleIndicesButHasSingleWriteAlias() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { SOURCE_1 },
                ALIAS_READ_WRITE_DEST,
                TEST_VALIDATIONS,
                listener
            ),
            (Boolean) null,
            e -> {
                assertEquals(1, e.validationErrors().size());
                assertThat(
                    e.validationErrors().get(0),
                    equalTo(
                        "no write index is defined for alias ["
                            + ALIAS_READ_WRITE_DEST
                            + "]. "
                            + "The write index may be explicitly disabled using is_write_index=false or the alias points "
                            + "to multiple indices without one being designated as a write index"
                    )
                );
            }
        );
    }

    public void testCheck_GivenDestIndexIsAliasThatIsIncludedInSource() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { SOURCE_1 },
                SOURCE_1_ALIAS,
                TEST_VALIDATIONS,
                listener
            ),
            (Boolean) null,
            e -> {
                assertEquals(1, e.validationErrors().size());
                assertThat(
                    e.validationErrors().get(0),
                    equalTo("Destination index [" + SOURCE_1 + "] is included in source expression [" + SOURCE_1 + "]")
                );
            }
        );

        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { SOURCE_1 },
                SOURCE_1_ALIAS,
                Collections.emptyList(),
                listener
            ),
            true,
            null
        );
    }

    public void testCheck_MultipleValidationErrors() throws InterruptedException {
        assertValidation(
            listener -> simpleNonRemoteValidator.validate(
                CLUSTER_STATE,
                new String[] { SOURCE_1, "missing" },
                SOURCE_1_ALIAS,
                TEST_VALIDATIONS,
                listener
            ),
            (Boolean) null,
            e -> {
                assertEquals(2, e.validationErrors().size());
                assertThat(e.validationErrors().get(0), equalTo("no such index [missing]"));
                assertThat(
                    e.validationErrors().get(1),
                    equalTo("Destination index [" + SOURCE_1 + "] is included in source expression [missing,source-1]")
                );
            }
        );
    }

    // CCS tests: at time of writing it wasn't possible to mock RemoteClusterService, therefore it's not possible
    // to test the whole validation but test RemoteSourceEnabledAndRemoteLicenseValidation
    public void testRemoteSourceBasic() throws InterruptedException {
        Context context = spy(
            new SourceDestValidator.Context(
                CLUSTER_STATE,
                new IndexNameExpressionResolver(),
                remoteClusterService,
                remoteClusterLicenseCheckerBasic,
                new String[] { REMOTE_BASIC + ":" + "SOURCE_1" },
                "dest",
                "node_id",
                "license"
            )
        );

        when(context.getRegisteredRemoteClusterNames()).thenReturn(Collections.singleton(REMOTE_BASIC));
        RemoteSourceEnabledAndRemoteLicenseValidation validator = new RemoteSourceEnabledAndRemoteLicenseValidation();

        assertValidationWithContext(
            listener -> validator.validate(context, listener),
            c -> { assertNull(c.getValidationException()); },
            null
        );
    }

    public void testRemoteSourcePlatinum() throws InterruptedException {
        final Context context = spy(
            new SourceDestValidator.Context(
                CLUSTER_STATE,
                new IndexNameExpressionResolver(),
                remoteClusterService,
                new RemoteClusterLicenseChecker(clientWithBasicLicense,
                    operationMode -> XPackLicenseState.isAllowedByOperationMode(operationMode, License.OperationMode.PLATINUM, true)),
                new String[] { REMOTE_BASIC + ":" + "SOURCE_1" },
                "dest",
                "node_id",
                "platinum"
            )
        );

        when(context.getRegisteredRemoteClusterNames()).thenReturn(Collections.singleton(REMOTE_BASIC));
        final RemoteSourceEnabledAndRemoteLicenseValidation validator = new RemoteSourceEnabledAndRemoteLicenseValidation();

        assertValidationWithContext(listener -> validator.validate(context, listener), c -> {
            assertNotNull(c.getValidationException());
            assertEquals(1, c.getValidationException().validationErrors().size());
            assertThat(
                c.getValidationException().validationErrors().get(0),
                equalTo(
                    "License check failed for remote cluster alias ["
                        + REMOTE_BASIC
                        + "], at least a [platinum] license is required, found license [basic]"
                )
            );
        }, null);

        final Context context2 = spy(
            new SourceDestValidator.Context(
                CLUSTER_STATE,
                new IndexNameExpressionResolver(),
                remoteClusterService,
                new RemoteClusterLicenseChecker(clientWithPlatinumLicense,
                    operationMode -> XPackLicenseState.isAllowedByOperationMode(operationMode, License.OperationMode.PLATINUM, true)),
                new String[] { REMOTE_PLATINUM + ":" + "SOURCE_1" },
                "dest",
                "node_id",
                "license"
            )
        );
        when(context2.getRegisteredRemoteClusterNames()).thenReturn(Collections.singleton(REMOTE_PLATINUM));

        assertValidationWithContext(
            listener -> validator.validate(context2, listener),
            c -> { assertNull(c.getValidationException()); },
            null
        );

        final Context context3 = spy(
            new SourceDestValidator.Context(
                CLUSTER_STATE,
                new IndexNameExpressionResolver(),
                remoteClusterService,
                new RemoteClusterLicenseChecker(clientWithPlatinumLicense,
                    operationMode -> XPackLicenseState.isAllowedByOperationMode(operationMode, License.OperationMode.PLATINUM, true)),
                new String[] { REMOTE_PLATINUM + ":" + "SOURCE_1" },
                "dest",
                "node_id",
                "platinum"
            )
        );
        when(context3.getRegisteredRemoteClusterNames()).thenReturn(Collections.singleton(REMOTE_PLATINUM));

        final RemoteSourceEnabledAndRemoteLicenseValidation validator3 = new RemoteSourceEnabledAndRemoteLicenseValidation();
        assertValidationWithContext(
            listener -> validator3.validate(context3, listener),
            c -> { assertNull(c.getValidationException()); },
            null
        );

        final Context context4 = spy(
            new SourceDestValidator.Context(
                CLUSTER_STATE,
                new IndexNameExpressionResolver(),
                remoteClusterService,
                new RemoteClusterLicenseChecker(clientWithTrialLicense,
                    operationMode -> XPackLicenseState.isAllowedByOperationMode(operationMode, License.OperationMode.PLATINUM, true)),
                new String[] { REMOTE_PLATINUM + ":" + "SOURCE_1" },
                "dest",
                "node_id",
                "trial"
            )
        );
        when(context4.getRegisteredRemoteClusterNames()).thenReturn(Collections.singleton(REMOTE_PLATINUM));

        final RemoteSourceEnabledAndRemoteLicenseValidation validator4 = new RemoteSourceEnabledAndRemoteLicenseValidation();
        assertValidationWithContext(
            listener -> validator4.validate(context4, listener),
            c -> { assertNull(c.getValidationException()); },
            null
        );
    }

    public void testRemoteSourceLicenseInActive() throws InterruptedException {
        final Context context = spy(
            new SourceDestValidator.Context(
                CLUSTER_STATE,
                new IndexNameExpressionResolver(),
                remoteClusterService,
                new RemoteClusterLicenseChecker(clientWithExpiredBasicLicense,
                    operationMode -> XPackLicenseState.isAllowedByOperationMode(operationMode, License.OperationMode.PLATINUM, true)),
                new String[] { REMOTE_BASIC + ":" + "SOURCE_1" },
                "dest",
                "node_id",
                "license"
            )
        );

        when(context.getRegisteredRemoteClusterNames()).thenReturn(Collections.singleton(REMOTE_BASIC));
        final RemoteSourceEnabledAndRemoteLicenseValidation validator = new RemoteSourceEnabledAndRemoteLicenseValidation();
        assertValidationWithContext(listener -> validator.validate(context, listener), c -> {
            assertNotNull(c.getValidationException());
            assertEquals(1, c.getValidationException().validationErrors().size());
            assertThat(
                c.getValidationException().validationErrors().get(0),
                equalTo("License check failed for remote cluster alias [" + REMOTE_BASIC + "], license is not active")
            );
        }, null);
    }

    public void testRemoteSourceDoesNotExist() throws InterruptedException {
        Context context = spy(
            new SourceDestValidator.Context(
                CLUSTER_STATE,
                new IndexNameExpressionResolver(),
                remoteClusterService,
                new RemoteClusterLicenseChecker(clientWithExpiredBasicLicense,
                    operationMode -> XPackLicenseState.isAllowedByOperationMode(operationMode, License.OperationMode.PLATINUM, true)),
                new String[] { "non_existing_remote:" + "SOURCE_1" },
                "dest",
                "node_id",
                "license"
            )
        );

        when(context.getRegisteredRemoteClusterNames()).thenReturn(Collections.singleton(REMOTE_BASIC));
        RemoteSourceEnabledAndRemoteLicenseValidation validator = new RemoteSourceEnabledAndRemoteLicenseValidation();

        assertValidationWithContext(listener -> validator.validate(context, listener), c -> {
            assertNotNull(c.getValidationException());
            assertEquals(1, c.getValidationException().validationErrors().size());
            assertThat(c.getValidationException().validationErrors().get(0), equalTo("no such remote cluster: [non_existing_remote]"));
        }, null);
    }

    public void testRequestValidation() {
        ActionRequestValidationException validationException = SourceDestValidator.validateRequest(null, "UPPERCASE");
        assertNotNull(validationException);
        assertEquals(1, validationException.validationErrors().size());
        assertThat(validationException.validationErrors().get(0), equalTo("Destination index [UPPERCASE] must be lowercase"));

        validationException = SourceDestValidator.validateRequest(null, "remote:dest");
        assertNotNull(validationException);
        assertEquals(1, validationException.validationErrors().size());
        assertThat(validationException.validationErrors().get(0), equalTo("Invalid index name [remote:dest], must not contain ':'"));

        validationException = SourceDestValidator.validateRequest(null, "dest");
        assertNull(validationException);

        validationException = new ActionRequestValidationException();
        validationException.addValidationError("error1");
        validationException.addValidationError("error2");
        validationException = SourceDestValidator.validateRequest(validationException, "dest");
        assertNotNull(validationException);
        assertEquals(2, validationException.validationErrors().size());
        assertEquals(validationException.validationErrors().get(0), "error1");
        assertEquals(validationException.validationErrors().get(1), "error2");

        validationException = SourceDestValidator.validateRequest(validationException, "UPPERCASE");
        assertNotNull(validationException);
        assertEquals(3, validationException.validationErrors().size());
        assertThat(validationException.validationErrors().get(2), equalTo("Destination index [UPPERCASE] must be lowercase"));
    }

    private <T> void assertValidation(Consumer<ActionListener<T>> function, T expected, Consumer<ValidationException> onException)
        throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            if (expected == null) {
                fail("expected an exception but got a response");
            } else {
                assertThat(r, equalTo(expected));
            }
        }, e -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            if (onException == null) {
                logger.error("got unexpected exception", e);
                fail("got unexpected exception: " + e.getMessage());
            } else if (e instanceof ValidationException) {
                onException.accept((ValidationException) e);
            } else {
                fail("got unexpected exception type: " + e);
            }
        }), latch);

        function.accept(listener);
        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
    }

    private void assertValidationWithContext(
        Consumer<ActionListener<Context>> function,
        CheckedConsumer<Context, ? extends Exception> onAnswer,
        Consumer<ValidationException> onException
    ) throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<Context> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            if (onAnswer == null) {
                fail("expected an exception but got a response");
            } else {
                onAnswer.accept(r);
            }
        }, e -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            if (onException == null) {
                logger.error("got unexpected exception", e);
                fail("got unexpected exception: " + e.getMessage());
            } else if (e instanceof ValidationException) {
                onException.accept((ValidationException) e);
            } else {
                fail("got unexpected exception type: " + e);
            }
        }), latch);

        function.accept(listener);
        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
    }
}
