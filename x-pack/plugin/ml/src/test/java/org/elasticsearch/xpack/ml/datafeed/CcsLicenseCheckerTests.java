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
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackInfoResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.XPackInfoAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CcsLicenseCheckerTests extends ESTestCase {

    public void testIsRemoteIndex() {
        List<String> indices = Arrays.asList("local-index1", "local-index2");
        assertFalse(CcsLicenseChecker.containsRemoteIndex(indices));
        indices = Arrays.asList("local-index1", "remote-cluster:remote-index2");
        assertTrue(CcsLicenseChecker.containsRemoteIndex(indices));
    }

    public void testRemoteClusterNames() {
        List<String> indices = Arrays.asList("local-index1", "local-index2");
        assertThat(CcsLicenseChecker.remoteClusterNames(indices), empty());
        indices = Arrays.asList("local-index1", "remote-cluster1:remote-index2");
        assertThat(CcsLicenseChecker.remoteClusterNames(indices), contains("remote-cluster1"));
        indices = Arrays.asList("remote-cluster1:index2", "index1", "remote-cluster2:index1");
        assertThat(CcsLicenseChecker.remoteClusterNames(indices), contains("remote-cluster1", "remote-cluster2"));
        indices = Arrays.asList("remote-cluster1:index2", "index1", "remote-cluster2:index1", "remote-cluster2:index2");
        assertThat(CcsLicenseChecker.remoteClusterNames(indices), contains("remote-cluster1", "remote-cluster2"));
    }

    public void testLicenseSupportsML() {
        XPackInfoResponse.LicenseInfo licenseInfo = new XPackInfoResponse.LicenseInfo("uid", "trial", "trial",
                License.Status.ACTIVE, randomNonNegativeLong());
        assertTrue(CcsLicenseChecker.licenseSupportsML(licenseInfo));

        licenseInfo = new XPackInfoResponse.LicenseInfo("uid", "trial", "trial", License.Status.EXPIRED, randomNonNegativeLong());
        assertFalse(CcsLicenseChecker.licenseSupportsML(licenseInfo));

        licenseInfo = new XPackInfoResponse.LicenseInfo("uid", "GOLD", "GOLD", License.Status.ACTIVE, randomNonNegativeLong());
        assertFalse(CcsLicenseChecker.licenseSupportsML(licenseInfo));

        licenseInfo = new XPackInfoResponse.LicenseInfo("uid", "PLATINUM", "PLATINUM", License.Status.ACTIVE, randomNonNegativeLong());
        assertTrue(CcsLicenseChecker.licenseSupportsML(licenseInfo));
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

        CcsLicenseChecker licenseChecker = new CcsLicenseChecker(client);
        AtomicReference<CcsLicenseChecker.LicenseViolation> licCheckResponse = new AtomicReference<>();

        licenseChecker.checkRemoteClusterLicenses(remoteClusterNames,
                new ActionListener<CcsLicenseChecker.LicenseViolation>() {
            @Override
            public void onResponse(CcsLicenseChecker.LicenseViolation response) {
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

        CcsLicenseChecker licenseChecker = new CcsLicenseChecker(client);
        AtomicReference<CcsLicenseChecker.LicenseViolation> licCheckResponse = new AtomicReference<>();

        licenseChecker.checkRemoteClusterLicenses(remoteClusterNames,
                new ActionListener<CcsLicenseChecker.LicenseViolation>() {
            @Override
            public void onResponse(CcsLicenseChecker.LicenseViolation response) {
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
        CcsLicenseChecker.RemoteClusterLicenseInfo info = new CcsLicenseChecker.RemoteClusterLicenseInfo("platinum-cluster", platinumLicence);
        assertEquals(Strings.toString(platinumLicence), CcsLicenseChecker.buildErrorMessage(info));

        XPackInfoResponse.LicenseInfo basicLicense = createBasicLicenseResponse();
        info = new CcsLicenseChecker.RemoteClusterLicenseInfo("basic-cluster", basicLicense);
        String expected = "The license mode on cluster [basic-cluster] with mode [BASIC] does not enable Machine Learning. "
                + Strings.toString(basicLicense);
        assertEquals(expected, CcsLicenseChecker.buildErrorMessage(info));

        XPackInfoResponse.LicenseInfo expiredLicense = createExpiredLicenseResponse();
        info = new CcsLicenseChecker.RemoteClusterLicenseInfo("expired-cluster", expiredLicense);
        expected = "The license on cluster [expired-cluster] is not active. " + Strings.toString(expiredLicense);
        assertEquals(expected, CcsLicenseChecker.buildErrorMessage(info));
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
        return new XPackInfoResponse.LicenseInfo("uid", "PLATINUM", "PLATINUM", License.Status.ACTIVE, randomNonNegativeLong());
    }

    private XPackInfoResponse.LicenseInfo createBasicLicenseResponse() {
        return new XPackInfoResponse.LicenseInfo("uid", "BASIC", "BASIC", License.Status.ACTIVE, randomNonNegativeLong());
    }

    private XPackInfoResponse.LicenseInfo createExpiredLicenseResponse() {
        return new XPackInfoResponse.LicenseInfo("uid", "PLATINUM", "PLATINUM", License.Status.EXPIRED, randomNonNegativeLong());
    }
}
