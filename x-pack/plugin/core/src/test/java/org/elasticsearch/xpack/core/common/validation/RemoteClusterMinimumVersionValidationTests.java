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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class RemoteClusterMinimumVersionValidationTests extends ESTestCase {

    private static final Version MIN_EXPECTED_VERSION = Version.V_7_11_0;
    private static final String REASON = "some reason";

    private Context context;

    @Before
    public void setUpMocks() {
        context = spy(new Context(null, null, null, null, null, null, null, null, null, null));
        doReturn(Version.V_7_10_2).when(context).getRemoteClusterVersion("cluster-A");
        doReturn(Version.V_7_11_0).when(context).getRemoteClusterVersion("cluster-B");
        doReturn(Version.V_7_11_2).when(context).getRemoteClusterVersion("cluster-C");
    }

    public void testValidate_NoRemoteClusters() {
        doReturn(Collections.emptySet()).when(context).getRegisteredRemoteClusterNames();
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION, REASON);
        validation.validate(
            context,
            ActionListener.wrap(
                ctx -> assertThat(ctx.getValidationException(), is(nullValue())),
                e -> fail(e.getMessage())));
    }

    public void testValidate_RemoteClustersVersionsOk() {
        doReturn(new HashSet<>(Arrays.asList("cluster-B", "cluster-C"))).when(context).getRegisteredRemoteClusterNames();
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION, REASON);
        validation.validate(
            context,
            ActionListener.wrap(
                ctx -> assertThat(ctx.getValidationException(), is(nullValue())),
                e -> fail(e.getMessage())));
    }

    public void testValidate_RemoteClusterVersionTooLow() {
        doReturn(new HashSet<>(Arrays.asList("cluster-A", "cluster-B", "cluster-C"))).when(context).getRegisteredRemoteClusterNames();
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION, REASON);
        validation.validate(
            context,
            ActionListener.wrap(
                ctx -> assertThat(
                    ctx.getValidationException().validationErrors(),
                    contains("remote clusters are expected to run at least version [7.11.0], but cluster [cluster-A] had version [7.10.2]; "
                        + "reason: [some reason]")),
                e -> fail(e.getMessage())));
    }

    public void testValidate_NoSuchRemoteCluster() {
        doReturn(new HashSet<>(Arrays.asList("cluster-B", "cluster-C", "cluster-D"))).when(context).getRegisteredRemoteClusterNames();
        doThrow(new NoSuchRemoteClusterException("cluster-D")).when(context).getRemoteClusterVersion("cluster-D");
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION, REASON);
        validation.validate(
            context,
            ActionListener.wrap(
                ctx -> assertThat(ctx.getValidationException().validationErrors(), contains("no such remote cluster: [cluster-D]")),
                e -> fail(e.getMessage())));
    }

    public void testValidate_OtherProblem() {
        doReturn(new HashSet<>(Arrays.asList("cluster-B", "cluster-C"))).when(context).getRegisteredRemoteClusterNames();
        doThrow(new IllegalArgumentException("some-other-problem")).when(context).getRemoteClusterVersion("cluster-C");
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION, REASON);
        validation.validate(
            context,
            ActionListener.wrap(
                ctx -> assertThat(
                    ctx.getValidationException().validationErrors(),
                    contains("Error resolving remote source: some-other-problem")),
                e -> fail(e.getMessage())));
    }
}
