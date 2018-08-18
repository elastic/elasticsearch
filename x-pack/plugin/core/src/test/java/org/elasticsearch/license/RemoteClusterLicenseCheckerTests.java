/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.XPackInfoAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class RemoteClusterLicenseCheckerTests extends ESTestCase {

    public void testNoRemoteIndex() {
        final List<String> indices = Arrays.asList("local-index1", "local-index2");
        assertFalse(RemoteClusterLicenseChecker.containsRemoteIndex(indices));
    }

    public void testRemoteIndex() {
        final List<String> indices = Arrays.asList("local-index1", "remote-cluster:remote-index2");
        assertTrue(RemoteClusterLicenseChecker.containsRemoteIndex(indices));
    }

    public void testNoRemoteIndices() {
        final List<String> indices = Collections.singletonList("local-index");
        assertThat(RemoteClusterLicenseChecker.remoteIndices(indices), is(empty()));
    }

    public void testRemoteIndices() {
        final List<String> indices = Arrays.asList("local-index", "remote-cluster:index1", "local-index2", "remote-cluster2:index1");
        assertThat(
                RemoteClusterLicenseChecker.remoteIndices(indices),
                containsInAnyOrder("remote-cluster:index1", "remote-cluster2:index1"));
    }

    public void testNoRemoteClusterNames() {
        final List<String> indices = Arrays.asList("local-index1", "local-index2");
        assertThat(RemoteClusterLicenseChecker.remoteClusterNames(indices), empty());
    }

    public void testOneRemoteClusterNames() {
        final List<String> indices = Arrays.asList("local-index1", "remote-cluster1:remote-index2");
        assertThat(RemoteClusterLicenseChecker.remoteClusterNames(indices), contains("remote-cluster1"));
    }

    public void testMoreThanOneRemoteClusterName() {
        final List<String> indices = Arrays.asList("remote-cluster1:index2", "index1", "remote-cluster2:index1");
        assertThat(RemoteClusterLicenseChecker.remoteClusterNames(indices), contains("remote-cluster1", "remote-cluster2"));
    }

    public void testDuplicateRemoteClusterNames() {
        final List<String> indices = Arrays.asList("remote-cluster1:index2", "index1", "remote-cluster2:index1", "remote-cluster2:index2");
        assertThat(RemoteClusterLicenseChecker.remoteClusterNames(indices), contains("remote-cluster1", "remote-cluster2"));
    }

    public void testCheckRemoteClusterLicensesGivenCompatibleLicenses() {
        final AtomicInteger index = new AtomicInteger(0);
        final List<XPackInfoResponse> responses = new ArrayList<>();

        final Client client = createMockClient();
        doAnswer(invocationMock -> {
            @SuppressWarnings("unchecked") ActionListener<XPackInfoResponse> listener =
                    (ActionListener<XPackInfoResponse>) invocationMock.getArguments()[2];
            listener.onResponse(responses.get(index.getAndIncrement()));
            return null;
        }).when(client).execute(same(XPackInfoAction.INSTANCE), any(), any());


        final List<String> remoteClusterNames = Arrays.asList("valid1", "valid2", "valid3");
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));

        final RemoteClusterLicenseChecker licenseChecker =
                new RemoteClusterLicenseChecker(client, RemoteClusterLicenseChecker::isLicensePlatinumOrTrial);
        final AtomicReference<RemoteClusterLicenseChecker.LicenseCheck> licenseCheck = new AtomicReference<>();

        licenseChecker.checkRemoteClusterLicenses(remoteClusterNames,
                new ActionListener<RemoteClusterLicenseChecker.LicenseCheck>() {

                    @Override
                    public void onResponse(RemoteClusterLicenseChecker.LicenseCheck response) {
                        licenseCheck.set(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail(e.getMessage());
                    }

                });

        verify(client, times(3)).execute(same(XPackInfoAction.INSTANCE), any(), any());
        assertNotNull(licenseCheck.get());
        assertTrue(licenseCheck.get().isSuccess());
    }

    public void testCheckRemoteClusterLicensesGivenIncompatibleLicense() {
        final AtomicInteger index = new AtomicInteger(0);
        final List<String> remoteClusterNames = Arrays.asList("good", "cluster-with-basic-license", "good2");
        final List<XPackInfoResponse> responses = new ArrayList<>();
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createBasicLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));

        final Client client = createMockClient();
        doAnswer(invocationMock -> {
            @SuppressWarnings("unchecked") ActionListener<XPackInfoResponse> listener =
                    (ActionListener<XPackInfoResponse>) invocationMock.getArguments()[2];
            listener.onResponse(responses.get(index.getAndIncrement()));
            return null;
        }).when(client).execute(same(XPackInfoAction.INSTANCE), any(), any());

        final RemoteClusterLicenseChecker licenseChecker =
                new RemoteClusterLicenseChecker(client, RemoteClusterLicenseChecker::isLicensePlatinumOrTrial);
        final AtomicReference<RemoteClusterLicenseChecker.LicenseCheck> licenseCheck = new AtomicReference<>();

        licenseChecker.checkRemoteClusterLicenses(
                remoteClusterNames,
                new ActionListener<RemoteClusterLicenseChecker.LicenseCheck>() {

                    @Override
                    public void onResponse(final RemoteClusterLicenseChecker.LicenseCheck response) {
                        licenseCheck.set(response);
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        fail(e.getMessage());
                    }

                });

        verify(client, times(2)).execute(same(XPackInfoAction.INSTANCE), any(), any());
        assertNotNull(licenseCheck.get());
        assertFalse(licenseCheck.get().isSuccess());
        assertThat(licenseCheck.get().remoteClusterLicenseInfo().clusterName(), equalTo("cluster-with-basic-license"));
        assertThat(licenseCheck.get().remoteClusterLicenseInfo().licenseInfo().getType(), equalTo("BASIC"));
    }


    public void testBuildErrorMessageForActiveCompatibleLicense() {
        final XPackInfoResponse.LicenseInfo platinumLicence = createPlatinumLicenseResponse();
        final RemoteClusterLicenseChecker.RemoteClusterLicenseInfo info =
                new RemoteClusterLicenseChecker.RemoteClusterLicenseInfo("platinum-cluster", platinumLicence);
        final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> RemoteClusterLicenseChecker.buildErrorMessage("", info, RemoteClusterLicenseChecker::isLicensePlatinumOrTrial));
        assertThat(e, hasToString(containsString("license must be incompatible to build error message")));
    }

    public void testBuildErrorMessageForIncompatibleLicense() {
        final XPackInfoResponse.LicenseInfo basicLicense = createBasicLicenseResponse();
        final RemoteClusterLicenseChecker.RemoteClusterLicenseInfo info =
                new RemoteClusterLicenseChecker.RemoteClusterLicenseInfo("basic-cluster", basicLicense);
        final String expected = "The license mode [BASIC] on cluster [basic-cluster] does not enable [Feature]. "
                + Strings.toString(basicLicense);
        assertThat(
                RemoteClusterLicenseChecker.buildErrorMessage("Feature", info, RemoteClusterLicenseChecker::isLicensePlatinumOrTrial),
                equalTo(expected));
    }

    public void testBuildErrorMessageForInactiveLicense() {
        final XPackInfoResponse.LicenseInfo expiredLicense = createExpiredLicenseResponse();
        final RemoteClusterLicenseChecker.RemoteClusterLicenseInfo info =
                new RemoteClusterLicenseChecker.RemoteClusterLicenseInfo("expired-cluster", expiredLicense);
        final String expected = "The license on cluster [expired-cluster] is not active. " + Strings.toString(expiredLicense);
        assertThat(
                RemoteClusterLicenseChecker.buildErrorMessage("Feature", info, RemoteClusterLicenseChecker::isLicensePlatinumOrTrial),
                equalTo(expected));
    }

    private Client createMockClient() {
        final Client client = mock(Client.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);
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
