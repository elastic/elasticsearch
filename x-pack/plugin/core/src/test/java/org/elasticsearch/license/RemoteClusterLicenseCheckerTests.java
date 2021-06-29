/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.XPackInfoAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class RemoteClusterLicenseCheckerTests extends ESTestCase {

    public void testIsNotRemoteIndex() {
        assertFalse(RemoteClusterLicenseChecker.isRemoteIndex("local-index"));
    }

    public void testIsRemoteIndex() {
        assertTrue(RemoteClusterLicenseChecker.isRemoteIndex("remote-cluster:remote-index"));
    }

    public void testNoRemoteIndex() {
        final List<String> indices = Arrays.asList("local-index1", "local-index2");
        assertFalse(RemoteClusterLicenseChecker.containsRemoteIndex(indices));
    }

    public void testRemoteIndex() {
        final List<String> indices = Arrays.asList("local-index", "remote-cluster:remote-index");
        assertTrue(RemoteClusterLicenseChecker.containsRemoteIndex(indices));
    }

    public void testNoRemoteIndices() {
        final List<String> indices = Collections.singletonList("local-index");
        assertThat(RemoteClusterLicenseChecker.remoteIndices(indices), is(empty()));
    }

    public void testRemoteIndices() {
        final List<String> indices = Arrays.asList("local-index1", "remote-cluster1:index1", "local-index2", "remote-cluster2:index1");
        assertThat(
                RemoteClusterLicenseChecker.remoteIndices(indices),
                containsInAnyOrder("remote-cluster1:index1", "remote-cluster2:index1"));
    }

    public void testNoRemoteClusterAliases() {
        final Set<String> remoteClusters = Sets.newHashSet("remote-cluster1", "remote-cluster2");
        final List<String> indices = Arrays.asList("local-index1", "local-index2");
        assertThat(RemoteClusterLicenseChecker.remoteClusterAliases(remoteClusters, indices), empty());
    }

    public void testOneRemoteClusterAlias() {
        final Set<String> remoteClusters = Sets.newHashSet("remote-cluster1", "remote-cluster2");
        final List<String> indices = Arrays.asList("local-index1", "remote-cluster1:remote-index1");
        assertThat(RemoteClusterLicenseChecker.remoteClusterAliases(remoteClusters, indices), contains("remote-cluster1"));
    }

    public void testMoreThanOneRemoteClusterAlias() {
        final Set<String> remoteClusters = Sets.newHashSet("remote-cluster1", "remote-cluster2");
        final List<String> indices = Arrays.asList("remote-cluster1:remote-index1", "local-index1", "remote-cluster2:remote-index1");
        assertThat(RemoteClusterLicenseChecker.remoteClusterAliases(remoteClusters, indices),
                containsInAnyOrder("remote-cluster1", "remote-cluster2"));
    }

    public void testDuplicateRemoteClusterAlias() {
        final Set<String> remoteClusters = Sets.newHashSet("remote-cluster1", "remote-cluster2");
        final List<String> indices = Arrays.asList(
                "remote-cluster1:remote-index1", "local-index1", "remote-cluster2:index1", "remote-cluster2:remote-index2");
        assertThat(RemoteClusterLicenseChecker.remoteClusterAliases(remoteClusters, indices),
                containsInAnyOrder("remote-cluster1", "remote-cluster2"));
    }

    public void testSimpleWildcardRemoteClusterAlias() {
        final Set<String> remoteClusters = Sets.newHashSet("remote-cluster1", "remote-cluster2");
        final List<String> indices = Arrays.asList("*:remote-index1", "local-index1");
        assertThat(RemoteClusterLicenseChecker.remoteClusterAliases(remoteClusters, indices),
                containsInAnyOrder("remote-cluster1", "remote-cluster2"));
    }

    public void testPartialWildcardRemoteClusterAlias() {
        final Set<String> remoteClusters = Sets.newHashSet("remote-cluster1", "remote-cluster2");
        final List<String> indices = Arrays.asList("*2:remote-index1", "local-index1");
        assertThat(RemoteClusterLicenseChecker.remoteClusterAliases(remoteClusters, indices), contains("remote-cluster2"));
    }

    public void testNonMatchingWildcardRemoteClusterAlias() {
        final Set<String> remoteClusters = Sets.newHashSet("remote-cluster1", "remote-cluster2");
        final List<String> indices = Arrays.asList("*3:remote-index1", "local-index1");
        assertThat(RemoteClusterLicenseChecker.remoteClusterAliases(remoteClusters, indices), empty());
    }

    public void testCheckRemoteClusterLicensesGivenCompatibleLicenses() {
        final AtomicInteger index = new AtomicInteger();
        final List<XPackInfoResponse> responses = new ArrayList<>();

        final ThreadPool threadPool = createMockThreadPool();
        final Client client = createMockClient(threadPool);
        doAnswer(invocationMock -> {
            @SuppressWarnings("unchecked") ActionListener<XPackInfoResponse> listener =
                    (ActionListener<XPackInfoResponse>) invocationMock.getArguments()[2];
            listener.onResponse(responses.get(index.getAndIncrement()));
            return null;
        }).when(client).execute(same(XPackInfoAction.INSTANCE), any(), any());

        final List<String> remoteClusterAliases = Arrays.asList("valid1", "valid2", "valid3");
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));

        final RemoteClusterLicenseChecker licenseChecker =
                new RemoteClusterLicenseChecker(client, operationMode ->
                    XPackLicenseState.isAllowedByOperationMode(operationMode, License.OperationMode.PLATINUM));
        final AtomicReference<RemoteClusterLicenseChecker.LicenseCheck> licenseCheck = new AtomicReference<>();

        licenseChecker.checkRemoteClusterLicenses(
                remoteClusterAliases,
                doubleInvocationProtectingListener(new ActionListener<RemoteClusterLicenseChecker.LicenseCheck>() {

                    @Override
                    public void onResponse(final RemoteClusterLicenseChecker.LicenseCheck response) {
                        licenseCheck.set(response);
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        fail(e.getMessage());
                    }

                }));

        verify(client, times(3)).execute(same(XPackInfoAction.INSTANCE), any(), any());
        assertNotNull(licenseCheck.get());
        assertTrue(licenseCheck.get().isSuccess());
    }

    public void testCheckRemoteClusterLicensesGivenIncompatibleLicense() {
        final AtomicInteger index = new AtomicInteger();
        final List<String> remoteClusterAliases = Arrays.asList("good", "cluster-with-basic-license", "good2");
        final List<XPackInfoResponse> responses = new ArrayList<>();
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createBasicLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));

        final ThreadPool threadPool = createMockThreadPool();
        final Client client = createMockClient(threadPool);
        doAnswer(invocationMock -> {
            @SuppressWarnings("unchecked") ActionListener<XPackInfoResponse> listener =
                    (ActionListener<XPackInfoResponse>) invocationMock.getArguments()[2];
            listener.onResponse(responses.get(index.getAndIncrement()));
            return null;
        }).when(client).execute(same(XPackInfoAction.INSTANCE), any(), any());

        final RemoteClusterLicenseChecker licenseChecker =
                new RemoteClusterLicenseChecker(client, operationMode ->
                    XPackLicenseState.isAllowedByOperationMode(operationMode, License.OperationMode.PLATINUM));
        final AtomicReference<RemoteClusterLicenseChecker.LicenseCheck> licenseCheck = new AtomicReference<>();

        licenseChecker.checkRemoteClusterLicenses(
                remoteClusterAliases,
                doubleInvocationProtectingListener(new ActionListener<RemoteClusterLicenseChecker.LicenseCheck>() {

                    @Override
                    public void onResponse(final RemoteClusterLicenseChecker.LicenseCheck response) {
                        licenseCheck.set(response);
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        fail(e.getMessage());
                    }

                }));

        verify(client, times(2)).execute(same(XPackInfoAction.INSTANCE), any(), any());
        assertNotNull(licenseCheck.get());
        assertFalse(licenseCheck.get().isSuccess());
        assertThat(licenseCheck.get().remoteClusterLicenseInfo().clusterAlias(), equalTo("cluster-with-basic-license"));
        assertThat(licenseCheck.get().remoteClusterLicenseInfo().licenseInfo().getType(), equalTo("BASIC"));
    }

    public void testCheckRemoteClusterLicencesGivenNonExistentCluster() {
        final AtomicInteger index = new AtomicInteger();
        final List<XPackInfoResponse> responses = new ArrayList<>();

        final List<String> remoteClusterAliases = Arrays.asList("valid1", "valid2", "valid3");
        final String failingClusterAlias = randomFrom(remoteClusterAliases);
        final ThreadPool threadPool = createMockThreadPool();
        final Client client = createMockClientThatThrowsOnGetRemoteClusterClient(threadPool, failingClusterAlias);
        doAnswer(invocationMock -> {
            @SuppressWarnings("unchecked") ActionListener<XPackInfoResponse> listener =
                    (ActionListener<XPackInfoResponse>) invocationMock.getArguments()[2];
            listener.onResponse(responses.get(index.getAndIncrement()));
            return null;
        }).when(client).execute(same(XPackInfoAction.INSTANCE), any(), any());

        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));

        final RemoteClusterLicenseChecker licenseChecker =
                new RemoteClusterLicenseChecker(client, operationMode ->
                    XPackLicenseState.isAllowedByOperationMode(operationMode, License.OperationMode.PLATINUM));
        final AtomicReference<Exception> exception = new AtomicReference<>();

        licenseChecker.checkRemoteClusterLicenses(
                remoteClusterAliases,
                doubleInvocationProtectingListener(new ActionListener<RemoteClusterLicenseChecker.LicenseCheck>() {

                    @Override
                    public void onResponse(final RemoteClusterLicenseChecker.LicenseCheck response) {
                        fail();
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        exception.set(e);
                    }

                }));

        assertNotNull(exception.get());
        assertThat(exception.get(), instanceOf(ElasticsearchException.class));
        assertThat(exception.get().getMessage(), equalTo("could not determine the license type for cluster [" + failingClusterAlias + "]"));
        assertNotNull(exception.get().getCause());
        assertThat(exception.get().getCause(), instanceOf(IllegalArgumentException.class));
    }

    public void testRemoteClusterLicenseCallUsesSystemContext() throws InterruptedException {
        final ThreadPool threadPool = new TestThreadPool(getTestName());

        try {
            final Client client = createMockClient(threadPool);
            doAnswer(invocationMock -> {
                assertTrue(threadPool.getThreadContext().isSystemContext());
                @SuppressWarnings("unchecked") ActionListener<XPackInfoResponse> listener =
                        (ActionListener<XPackInfoResponse>) invocationMock.getArguments()[2];
                listener.onResponse(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
                return null;
            }).when(client).execute(same(XPackInfoAction.INSTANCE), any(), any());

            final RemoteClusterLicenseChecker licenseChecker =
                    new RemoteClusterLicenseChecker(client, operationMode ->
                        XPackLicenseState.isAllowedByOperationMode(operationMode, License.OperationMode.PLATINUM));

            final List<String> remoteClusterAliases = Collections.singletonList("valid");
            licenseChecker.checkRemoteClusterLicenses(
                    remoteClusterAliases, doubleInvocationProtectingListener(ActionListener.wrap(() -> {})));

            verify(client, times(1)).execute(same(XPackInfoAction.INSTANCE), any(), any());
        } finally {
            terminate(threadPool);
        }
    }

    public void testListenerIsExecutedWithCallingContext() throws InterruptedException {
        final AtomicInteger index = new AtomicInteger();
        final List<XPackInfoResponse> responses = new ArrayList<>();

        final ThreadPool threadPool = new TestThreadPool(getTestName());

        try {
            final List<String> remoteClusterAliases = Arrays.asList("valid1", "valid2", "valid3");
            final Client client;
            final boolean failure = randomBoolean();
            if (failure) {
                client = createMockClientThatThrowsOnGetRemoteClusterClient(threadPool, randomFrom(remoteClusterAliases));
            } else {
                client = createMockClient(threadPool);
            }
            doAnswer(invocationMock -> {
                @SuppressWarnings("unchecked") ActionListener<XPackInfoResponse> listener =
                        (ActionListener<XPackInfoResponse>) invocationMock.getArguments()[2];
                listener.onResponse(responses.get(index.getAndIncrement()));
                return null;
            }).when(client).execute(same(XPackInfoAction.INSTANCE), any(), any());

            responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
            responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
            responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));

            final RemoteClusterLicenseChecker licenseChecker =
                    new RemoteClusterLicenseChecker(client, operationMode ->
                        XPackLicenseState.isAllowedByOperationMode(operationMode, License.OperationMode.PLATINUM));

            final AtomicBoolean listenerInvoked = new AtomicBoolean();
            threadPool.getThreadContext().putHeader("key", "value");
            licenseChecker.checkRemoteClusterLicenses(
                    remoteClusterAliases,
                    doubleInvocationProtectingListener(new ActionListener<RemoteClusterLicenseChecker.LicenseCheck>() {

                        @Override
                        public void onResponse(final RemoteClusterLicenseChecker.LicenseCheck response) {
                            if (failure) {
                                fail();
                            }
                            assertThat(threadPool.getThreadContext().getHeader("key"), equalTo("value"));
                            assertFalse(threadPool.getThreadContext().isSystemContext());
                            listenerInvoked.set(true);
                        }

                        @Override
                        public void onFailure(final Exception e) {
                            if (failure == false) {
                                fail();
                            }
                            assertThat(threadPool.getThreadContext().getHeader("key"), equalTo("value"));
                            assertFalse(threadPool.getThreadContext().isSystemContext());
                            listenerInvoked.set(true);
                        }

                    }));

            assertTrue(listenerInvoked.get());
        } finally {
            terminate(threadPool);
        }
    }

    public void testBuildErrorMessageForActiveCompatibleLicense() {
        final XPackInfoResponse.LicenseInfo platinumLicence = createPlatinumLicenseResponse();
        final RemoteClusterLicenseChecker.RemoteClusterLicenseInfo info =
                new RemoteClusterLicenseChecker.RemoteClusterLicenseInfo("platinum-cluster", platinumLicence);
        final AssertionError e = expectThrows(
                AssertionError.class,
                () -> RemoteClusterLicenseChecker.buildErrorMessage("", info, RemoteClusterLicenseChecker::isAllowedByLicense));
        assertThat(e, hasToString(containsString("license must be incompatible to build error message")));
    }

    public void testBuildErrorMessageForIncompatibleLicense() {
        final XPackInfoResponse.LicenseInfo basicLicense = createBasicLicenseResponse();
        final RemoteClusterLicenseChecker.RemoteClusterLicenseInfo info =
                new RemoteClusterLicenseChecker.RemoteClusterLicenseInfo("basic-cluster", basicLicense);
        assertThat(
                RemoteClusterLicenseChecker.buildErrorMessage("Feature", info, RemoteClusterLicenseChecker::isAllowedByLicense),
                equalTo("the license mode [BASIC] on cluster [basic-cluster] does not enable [Feature]"));
    }

    public void testBuildErrorMessageForInactiveLicense() {
        final XPackInfoResponse.LicenseInfo expiredLicense = createExpiredLicenseResponse();
        final RemoteClusterLicenseChecker.RemoteClusterLicenseInfo info =
                new RemoteClusterLicenseChecker.RemoteClusterLicenseInfo("expired-cluster", expiredLicense);
        assertThat(
                RemoteClusterLicenseChecker.buildErrorMessage("Feature", info, RemoteClusterLicenseChecker::isAllowedByLicense),
                equalTo("the license on cluster [expired-cluster] is not active"));
    }

    public void testCheckRemoteClusterLicencesNoLicenseMetadata() {
        final ThreadPool threadPool = createMockThreadPool();
        final Client client = createMockClient(threadPool);
        doAnswer(invocationMock -> {
            @SuppressWarnings("unchecked") ActionListener<XPackInfoResponse> listener =
                (ActionListener<XPackInfoResponse>) invocationMock.getArguments()[2];
            listener.onResponse(new XPackInfoResponse(null, null, null));
            return null;
        }).when(client).execute(same(XPackInfoAction.INSTANCE), any(), any());

        final RemoteClusterLicenseChecker licenseChecker =
            new RemoteClusterLicenseChecker(client, operationMode ->
                XPackLicenseState.isAllowedByOperationMode(operationMode, License.OperationMode.PLATINUM));
        final AtomicReference<Exception> exception = new AtomicReference<>();

        licenseChecker.checkRemoteClusterLicenses(
            Collections.singletonList("remote"),
            doubleInvocationProtectingListener(new ActionListener<RemoteClusterLicenseChecker.LicenseCheck>() {

                @Override
                public void onResponse(final RemoteClusterLicenseChecker.LicenseCheck response) {
                    fail();
                }

                @Override
                public void onFailure(final Exception e) {
                    exception.set(e);
                }

            }));

        assertNotNull(exception.get());
        assertThat(exception.get(), instanceOf(ResourceNotFoundException.class));
        assertThat(exception.get().getMessage(), equalTo("license info is missing for cluster [remote]"));
    }

    private ActionListener<RemoteClusterLicenseChecker.LicenseCheck> doubleInvocationProtectingListener(
            final ActionListener<RemoteClusterLicenseChecker.LicenseCheck> listener) {
        final AtomicBoolean listenerInvoked = new AtomicBoolean();
        return new ActionListener<RemoteClusterLicenseChecker.LicenseCheck>() {

            @Override
            public void onResponse(final RemoteClusterLicenseChecker.LicenseCheck response) {
                if (listenerInvoked.compareAndSet(false, true) == false) {
                    fail("listener invoked twice");
                }
                listener.onResponse(response);
            }

            @Override
            public void onFailure(final Exception e) {
                if (listenerInvoked.compareAndSet(false, true) == false) {
                    fail("listener invoked twice");
                }
                listener.onFailure(e);
            }

        };
    }

    private ThreadPool createMockThreadPool() {
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        return threadPool;
    }

    private Client createMockClient(final ThreadPool threadPool) {
        return createMockClient(threadPool, client -> when(client.getRemoteClusterClient(anyString())).thenReturn(client));
    }

    private Client createMockClientThatThrowsOnGetRemoteClusterClient(final ThreadPool threadPool, final String clusterAlias) {
        return createMockClient(
                threadPool,
                client -> {
                    when(client.getRemoteClusterClient(clusterAlias)).thenThrow(new IllegalArgumentException());
                    when(client.getRemoteClusterClient(argThat(not(clusterAlias)))).thenReturn(client);
                });
    }

    private Client createMockClient(final ThreadPool threadPool, final Consumer<Client> finish) {
        final Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        finish.accept(client);
        return client;
    }

    private XPackInfoResponse.LicenseInfo createPlatinumLicenseResponse() {
        return new XPackInfoResponse.LicenseInfo("uid", "PLATINUM", "PLATINUM", LicenseStatus.ACTIVE, randomNonNegativeLong());
    }

    private XPackInfoResponse.LicenseInfo createBasicLicenseResponse() {
        return new XPackInfoResponse.LicenseInfo("uid", "BASIC", "BASIC", LicenseStatus.ACTIVE, randomNonNegativeLong());
    }

    private XPackInfoResponse.LicenseInfo createExpiredLicenseResponse() {
        return new XPackInfoResponse.LicenseInfo("uid", "PLATINUM", "PLATINUM", LicenseStatus.EXPIRED, randomNonNegativeLong());
    }

}
