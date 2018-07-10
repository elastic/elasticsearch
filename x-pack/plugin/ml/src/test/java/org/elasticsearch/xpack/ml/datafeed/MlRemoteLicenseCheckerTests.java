/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.protocol.license.LicenseStatus;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MlRemoteLicenseCheckerTests extends ESTestCase {

    public void testIsRemoteIndex() {
        List<String> indices = Arrays.asList("local-index1", "local-index2");
        assertFalse(MlRemoteLicenseChecker.containsRemoteIndex(indices));
        indices = Arrays.asList("local-index1", "remote-cluster:remote-index2");
        assertTrue(MlRemoteLicenseChecker.containsRemoteIndex(indices));
    }

    public void testRemoteIndices() {
        List<String> indices = Collections.singletonList("local-index");
        assertThat(MlRemoteLicenseChecker.remoteIndices(indices), is(empty()));
        indices = Arrays.asList("local-index", "remote-cluster:index1", "local-index2", "remote-cluster2:index1");
        assertThat(MlRemoteLicenseChecker.remoteIndices(indices), containsInAnyOrder("remote-cluster:index1", "remote-cluster2:index1"));
    }

    public void testRemoteClusterNames() {
        List<String> indices = Arrays.asList("local-index1", "local-index2");
        assertThat(MlRemoteLicenseChecker.remoteClusterNames(indices), empty());
        indices = Arrays.asList("local-index1", "remote-cluster1:remote-index2");
        assertThat(MlRemoteLicenseChecker.remoteClusterNames(indices), contains("remote-cluster1"));
        indices = Arrays.asList("remote-cluster1:index2", "index1", "remote-cluster2:index1");
        assertThat(MlRemoteLicenseChecker.remoteClusterNames(indices), contains("remote-cluster1", "remote-cluster2"));
        indices = Arrays.asList("remote-cluster1:index2", "index1", "remote-cluster2:index1", "remote-cluster2:index2");
        assertThat(MlRemoteLicenseChecker.remoteClusterNames(indices), contains("remote-cluster1", "remote-cluster2"));
    }

    public void testLicenseSupportsML() {
        XPackInfoResponse.LicenseInfo licenseInfo = new XPackInfoResponse.LicenseInfo("uid", "trial", "trial",
                LicenseStatus.ACTIVE, randomNonNegativeLong());
        assertTrue(MlRemoteLicenseChecker.licenseSupportsML(licenseInfo));

        licenseInfo = new XPackInfoResponse.LicenseInfo("uid", "trial", "trial", LicenseStatus.EXPIRED, randomNonNegativeLong());
        assertFalse(MlRemoteLicenseChecker.licenseSupportsML(licenseInfo));

        licenseInfo = new XPackInfoResponse.LicenseInfo("uid", "GOLD", "GOLD", LicenseStatus.ACTIVE, randomNonNegativeLong());
        assertFalse(MlRemoteLicenseChecker.licenseSupportsML(licenseInfo));

        licenseInfo = new XPackInfoResponse.LicenseInfo("uid", "PLATINUM", "PLATINUM", LicenseStatus.ACTIVE, randomNonNegativeLong());
        assertTrue(MlRemoteLicenseChecker.licenseSupportsML(licenseInfo));
    }

    public void testCheckRemoteClusterLicenses_givenValidLicenses() {
        final AtomicInteger index = new AtomicInteger(0);
        final List<XPackInfoResponse> responses = new ArrayList<>();

        Client client = createMockClient();
        doAnswer(invocationMock -> {
            @SuppressWarnings("raw_types")
            ActionListener listener = (ActionListener) invocationMock.getArguments()[2];
            listener.onResponse(responses.get(index.getAndIncrement()));
            return null;
        }).when(client).execute(same(XPackInfoAction.INSTANCE), any(), any());


        List<String> remoteClusterNames = Arrays.asList("valid1", "valid2", "valid3");
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));

        MlRemoteLicenseChecker licenseChecker = new MlRemoteLicenseChecker(client);
        AtomicReference<MlRemoteLicenseChecker.LicenseViolation> licCheckResponse = new AtomicReference<>();

        licenseChecker.checkRemoteClusterLicenses(remoteClusterNames,
                new ActionListener<MlRemoteLicenseChecker.LicenseViolation>() {
            @Override
            public void onResponse(MlRemoteLicenseChecker.LicenseViolation response) {
                licCheckResponse.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                fail(e.getMessage());
            }
        });

        verify(client, times(3)).execute(same(XPackInfoAction.INSTANCE), any(), any());
        assertNotNull(licCheckResponse.get());
        assertFalse(licCheckResponse.get().isViolated());
        assertNull(licCheckResponse.get().get());
    }

    public void testCheckRemoteClusterLicenses_givenInvalidLicense() {
        final AtomicInteger index = new AtomicInteger(0);
        List<String> remoteClusterNames = Arrays.asList("good", "cluster-with-basic-license", "good2");
        final List<XPackInfoResponse> responses = new ArrayList<>();
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createBasicLicenseResponse(), null));
        responses.add(new XPackInfoResponse(null, createPlatinumLicenseResponse(), null));

        Client client = createMockClient();
        doAnswer(invocationMock -> {
            @SuppressWarnings("raw_types")
            ActionListener listener = (ActionListener) invocationMock.getArguments()[2];
            listener.onResponse(responses.get(index.getAndIncrement()));
            return null;
        }).when(client).execute(same(XPackInfoAction.INSTANCE), any(), any());

        MlRemoteLicenseChecker licenseChecker = new MlRemoteLicenseChecker(client);
        AtomicReference<MlRemoteLicenseChecker.LicenseViolation> licCheckResponse = new AtomicReference<>();

        licenseChecker.checkRemoteClusterLicenses(remoteClusterNames,
                new ActionListener<MlRemoteLicenseChecker.LicenseViolation>() {
            @Override
            public void onResponse(MlRemoteLicenseChecker.LicenseViolation response) {
                licCheckResponse.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                fail(e.getMessage());
            }
        });

        verify(client, times(2)).execute(same(XPackInfoAction.INSTANCE), any(), any());
        assertNotNull(licCheckResponse.get());
        assertTrue(licCheckResponse.get().isViolated());
        assertEquals("cluster-with-basic-license", licCheckResponse.get().get().getClusterName());
        assertEquals("BASIC", licCheckResponse.get().get().getLicenseInfo().getType());
    }

    public void testBuildErrorMessage() {
        XPackInfoResponse.LicenseInfo platinumLicence = createPlatinumLicenseResponse();
        MlRemoteLicenseChecker.RemoteClusterLicenseInfo info =
                new MlRemoteLicenseChecker.RemoteClusterLicenseInfo("platinum-cluster", platinumLicence);
        assertEquals(Strings.toString(platinumLicence), MlRemoteLicenseChecker.buildErrorMessage(info));

        XPackInfoResponse.LicenseInfo basicLicense = createBasicLicenseResponse();
        info = new MlRemoteLicenseChecker.RemoteClusterLicenseInfo("basic-cluster", basicLicense);
        String expected = "The license mode [BASIC] on cluster [basic-cluster] does not enable Machine Learning. "
                + Strings.toString(basicLicense);
        assertEquals(expected, MlRemoteLicenseChecker.buildErrorMessage(info));

        XPackInfoResponse.LicenseInfo expiredLicense = createExpiredLicenseResponse();
        info = new MlRemoteLicenseChecker.RemoteClusterLicenseInfo("expired-cluster", expiredLicense);
        expected = "The license on cluster [expired-cluster] is not active. " + Strings.toString(expiredLicense);
        assertEquals(expected, MlRemoteLicenseChecker.buildErrorMessage(info));
    }

    private Client createMockClient() {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
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
