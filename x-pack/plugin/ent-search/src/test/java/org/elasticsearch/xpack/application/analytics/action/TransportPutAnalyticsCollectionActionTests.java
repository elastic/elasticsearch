/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollectionService;
import org.elasticsearch.xpack.application.utils.LicenseUtils;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPutAnalyticsCollectionActionTests extends ESTestCase {
    @SuppressWarnings("unchecked")
    public void testWithSupportedLicense() {
        AnalyticsCollectionService analyticsCollectionService = mock(AnalyticsCollectionService.class);

        TransportPutAnalyticsCollectionAction transportAction = createTransportAction(mockLicenseState(true), analyticsCollectionService);
        PutAnalyticsCollectionAction.Request request = mock(PutAnalyticsCollectionAction.Request.class);

        ClusterState clusterState = mock(ClusterState.class);

        ActionListener<PutAnalyticsCollectionAction.Response> listener = mock(ActionListener.class);

        transportAction.masterOperation(mock(Task.class), request, clusterState, listener);
        verify(analyticsCollectionService, times(1)).putAnalyticsCollection(clusterState, request, listener);
        verify(listener, never()).onFailure(any());
    }

    public void testWithUnsupportedLicense() {
        AnalyticsCollectionService analyticsCollectionService = mock(AnalyticsCollectionService.class);

        TransportPutAnalyticsCollectionAction transportAction = createTransportAction(mockLicenseState(false), analyticsCollectionService);
        PutAnalyticsCollectionAction.Request request = mock(PutAnalyticsCollectionAction.Request.class);

        ClusterState clusterState = mock(ClusterState.class);

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutAnalyticsCollectionAction.Response> responseRef = new AtomicReference<>();
        ActionListener<PutAnalyticsCollectionAction.Response> listener = ActionListener.wrap(
            r -> responseRef.set(r),
            e -> throwableRef.set(e)
        );

        transportAction.masterOperation(mock(Task.class), request, clusterState, listener);

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(ElasticsearchSecurityException.class));
        assertThat(
            throwableRef.get().getMessage(),
            containsString("Search Applications and behavioral analytics require an active trial, platinum or enterprise license.")
        );

        verify(analyticsCollectionService, never()).putAnalyticsCollection(any(), any(), any());
    }

    private MockLicenseState mockLicenseState(boolean supported) {
        MockLicenseState licenseState = mock(MockLicenseState.class);

        when(licenseState.isAllowed(LicenseUtils.LICENSED_ENT_SEARCH_FEATURE)).thenReturn(supported);
        when(licenseState.isActive()).thenReturn(supported);
        when(licenseState.statusDescription()).thenReturn("invalid license");

        return licenseState;
    }

    private TransportPutAnalyticsCollectionAction createTransportAction(
        XPackLicenseState licenseState,
        AnalyticsCollectionService analyticsCollectionService
    ) {
        return new TransportPutAnalyticsCollectionAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            analyticsCollectionService,
            licenseState
        );
    }
}
