/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.validation;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.Context;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.RemoteClusterMinimumVersionValidation;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.SourceDestValidation;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeSet;

import static java.util.Collections.emptySet;
import static java.util.Collections.emptySortedSet;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class RemoteClusterMinimumVersionValidationTests extends ESTestCase {

    private static final TransportVersion MIN_EXPECTED_VERSION = TransportVersions.V_8_11_X;
    private static final String REASON = "some reason";

    private Context context;

    @Before
    public void setUpMocks() {
        context = spy(new Context(null, null, null, null, null, null, null, null, null, null));
        doReturn(TransportVersions.V_8_10_X).when(context).getRemoteClusterVersion("cluster-A");
        doReturn(TransportVersions.V_8_11_X).when(context).getRemoteClusterVersion("cluster-B");
        doReturn(TransportVersions.V_8_12_0).when(context).getRemoteClusterVersion("cluster-C");
    }

    public void testGetters() {
        RemoteClusterMinimumVersionValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION, REASON);
        assertThat(validation.getMinExpectedTransportVersion(), is(equalTo(MIN_EXPECTED_VERSION)));
        assertThat(validation.getReason(), is(equalTo(REASON)));
    }

    public void testValidate_NoRemoteClusters() {
        doReturn(emptySet()).when(context).getRegisteredRemoteClusterNames();
        doReturn(emptySortedSet()).when(context).resolveRemoteSource();
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION, REASON);
        validation.validate(
            context,
            ActionTestUtils.assertNoFailureListener(ctx -> assertThat(ctx.getValidationException(), is(nullValue())))
        );
    }

    public void testValidate_RemoteClustersVersionsOk() {
        doReturn(new HashSet<>(Arrays.asList("cluster-B", "cluster-C"))).when(context).getRegisteredRemoteClusterNames();
        doReturn(new TreeSet<>(Arrays.asList("cluster-B:dummy", "cluster-C:dummy"))).when(context).resolveRemoteSource();
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION, REASON);
        validation.validate(
            context,
            ActionTestUtils.assertNoFailureListener(ctx -> assertThat(ctx.getValidationException(), is(nullValue())))
        );
    }

    public void testValidate_OneRemoteClusterVersionTooLow() {
        doReturn(new HashSet<>(Arrays.asList("cluster-A", "cluster-B", "cluster-C"))).when(context).getRegisteredRemoteClusterNames();
        doReturn(new TreeSet<>(Arrays.asList("cluster-A:dummy", "cluster-B:dummy", "cluster-C:dummy"))).when(context).resolveRemoteSource();
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION, REASON);
        validation.validate(
            context,
            ActionTestUtils.assertNoFailureListener(
                ctx -> assertThat(
                    ctx.getValidationException().validationErrors(),
                    contains(
                        "remote clusters are expected to run at least version [8.11.0-8.11.4] (reason: [some reason]), "
                            + "but the following clusters were too old: [cluster-A (8.10.0-8.10.4)]"
                    )
                )
            )
        );
    }

    public void testValidate_TwoRemoteClusterVersionsTooLow() {
        doReturn(new HashSet<>(Arrays.asList("cluster-A", "cluster-B", "cluster-C"))).when(context).getRegisteredRemoteClusterNames();
        doReturn(new TreeSet<>(Arrays.asList("cluster-A:dummy", "cluster-B:dummy", "cluster-C:dummy"))).when(context).resolveRemoteSource();
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(TransportVersions.V_8_12_0, REASON);
        validation.validate(
            context,
            ActionTestUtils.assertNoFailureListener(
                ctx -> assertThat(
                    ctx.getValidationException().validationErrors(),
                    contains(
                        "remote clusters are expected to run at least version [8.12.0] (reason: [some reason]), "
                            + "but the following clusters were too old: [cluster-A (8.10.0-8.10.4), cluster-B (8.11.0-8.11.4)]"
                    )
                )
            )
        );
    }

    public void testValidate_NoSuchRemoteCluster() {
        doReturn(new HashSet<>(Arrays.asList("cluster-B", "cluster-C", "cluster-D"))).when(context).getRegisteredRemoteClusterNames();
        doReturn(new TreeSet<>(Arrays.asList("cluster-B:dummy", "cluster-C:dummy", "cluster-D:dummy"))).when(context).resolveRemoteSource();
        doThrow(new NoSuchRemoteClusterException("cluster-D")).when(context).getRemoteClusterVersion("cluster-D");
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION, REASON);
        validation.validate(
            context,
            ActionTestUtils.assertNoFailureListener(
                ctx -> assertThat(ctx.getValidationException().validationErrors(), contains("no such remote cluster: [cluster-D]"))
            )
        );
    }

    public void testValidate_OtherProblem() {
        doReturn(new HashSet<>(Arrays.asList("cluster-B", "cluster-C"))).when(context).getRegisteredRemoteClusterNames();
        doReturn(new TreeSet<>(Arrays.asList("cluster-B:dummy", "cluster-C:dummy"))).when(context).resolveRemoteSource();
        doThrow(new IllegalArgumentException("some-other-problem")).when(context).getRemoteClusterVersion("cluster-C");
        SourceDestValidation validation = new RemoteClusterMinimumVersionValidation(MIN_EXPECTED_VERSION, REASON);
        validation.validate(
            context,
            ActionTestUtils.assertNoFailureListener(
                ctx -> assertThat(
                    ctx.getValidationException().validationErrors(),
                    contains("Error resolving remote source: some-other-problem")
                )
            )
        );
    }
}
