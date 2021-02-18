/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.validation;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.Context;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.RemoteClusterMinimumVersionValidation;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.SourceDestValidation;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteClusterMinimumVersionValidationTests extends ESTestCase {

    private static final Version MIN_EXPECTED_VERSION = Version.V_7_11_0;

    private Context context;
    private RemoteClusterService remoteClusterService;
    private Transport.Connection connectionA;  // 7.10.2
    private Transport.Connection connectionB;  // 7.11.0
    private Transport.Connection connectionC;  // 7.11.2

    @Before
    public void setUpMocks() {
        connectionA = mock(Transport.Connection.class);
        when(connectionA.getVersion()).thenReturn(Version.V_7_10_2);
        connectionB = mock(Transport.Connection.class);
        when(connectionB.getVersion()).thenReturn(Version.V_7_11_0);
        connectionC = mock(Transport.Connection.class);
        when(connectionC.getVersion()).thenReturn(Version.V_7_11_2);
        remoteClusterService = mock(RemoteClusterService.class);
        when(remoteClusterService.getConnection("cluster-A")).thenReturn(connectionA);
        when(remoteClusterService.getConnection("cluster-B")).thenReturn(connectionB);
        when(remoteClusterService.getConnection("cluster-C")).thenReturn(connectionC);
        context = new Context(null, null, remoteClusterService, null, null, null, null, null, null, null);
    }

    public void testValidate_NoRemoteClusters() {
        when(remoteClusterService.getRegisteredRemoteClusterNames()).thenReturn(Collections.emptySet());
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION);
        validation.validate(
            context,
            ActionListener.wrap(
                ctx -> assertThat(ctx.getValidationException(), is(nullValue())),
                e -> fail(e.getMessage())));
    }

    public void testValidate_RemoteClustersVersionsOk() {
        when(remoteClusterService.getRegisteredRemoteClusterNames()).thenReturn(new HashSet<>(Arrays.asList("cluster-B", "cluster-C")));
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION);
        validation.validate(
            context,
            ActionListener.wrap(
                ctx -> assertThat(ctx.getValidationException(), is(nullValue())),
                e -> fail(e.getMessage())));
    }

    public void testValidate_RemoteClusterVersionTooLow() {
        when(remoteClusterService.getRegisteredRemoteClusterNames())
            .thenReturn(new HashSet<>(Arrays.asList("cluster-A", "cluster-B", "cluster-C")));
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION);
        validation.validate(
            context,
            ActionListener.wrap(
                ctx -> assertThat(
                    ctx.getValidationException().validationErrors(),
                    contains(
                        "remote clusters are expected to run at least version [7.11.0], but cluster [cluster-A] had version [7.10.2]")),
                e -> fail(e.getMessage())));
    }

    public void testValidate_NoSuchRemoteCluster() {
        when(remoteClusterService.getRegisteredRemoteClusterNames())
            .thenReturn(new HashSet<>(Arrays.asList("cluster-B", "cluster-C", "cluster-D")));
        when(remoteClusterService.getConnection("cluster-D")).thenThrow(new NoSuchRemoteClusterException("cluster-D"));
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION);
        validation.validate(
            context,
            ActionListener.wrap(
                ctx -> assertThat(ctx.getValidationException().validationErrors(), contains("no such remote cluster: [cluster-D]")),
                e -> fail(e.getMessage())));
    }

    public void testValidate_OtherProblem() {
        when(remoteClusterService.getRegisteredRemoteClusterNames()).thenReturn(new HashSet<>(Arrays.asList("cluster-B", "cluster-C")));
        when(remoteClusterService.getConnection("cluster-C")).thenThrow(new IllegalArgumentException("some-other-problem"));
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION);
        validation.validate(
            context,
            ActionListener.wrap(
                ctx -> assertThat(
                    ctx.getValidationException().validationErrors(),
                    contains("Error resolving remote source: some-other-problem")),
                e -> fail(e.getMessage())));
    }
}
