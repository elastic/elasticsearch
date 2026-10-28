/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.core.action.SetUpgradeModeActionRequest;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.action.SetTransformUpgradeModeAction;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TransformTests extends ESTestCase {

    public void testSetTransformUpgradeMode() {
        var threadPool = new TestThreadPool("testSetTransformUpgradeMode");
        Client client = mock();
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            ActionListener<AcknowledgedResponse> listener = invocationOnMock.getArgument(2);
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(client).execute(same(SetTransformUpgradeModeAction.INSTANCE), any(), any());

        try (var transformPlugin = new Transform(Settings.EMPTY)) {
            SetOnce<Map<String, Object>> response = new SetOnce<>();
            transformPlugin.prepareForIndicesMigration(emptyProject(), client, ActionTestUtils.assertNoFailureListener(response::set));

            assertThat(response.get(), equalTo(Collections.singletonMap("already_in_upgrade_mode", false)));
            verify(client).execute(
                same(SetTransformUpgradeModeAction.INSTANCE),
                eq(new SetUpgradeModeActionRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, true)),
                any()
            );

            transformPlugin.indicesMigrationComplete(
                response.get(),
                client,
                ActionTestUtils.assertNoFailureListener(ESTestCase::assertTrue)
            );

            verify(client).execute(
                same(SetTransformUpgradeModeAction.INSTANCE),
                eq(new SetUpgradeModeActionRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, false)),
                any()
            );
        } finally {
            terminate(threadPool);
        }
    }

    public void testIgnoreSetTransformUpgradeMode() {
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .putCustom(TransformMetadata.TYPE, new TransformMetadata.Builder().upgradeMode(true).build())
            .build();
        Client client = mock();

        try (var transformPlugin = new Transform(Settings.EMPTY)) {
            SetOnce<Map<String, Object>> response = new SetOnce<>();
            transformPlugin.prepareForIndicesMigration(project, client, ActionTestUtils.assertNoFailureListener(response::set));

            assertThat(response.get(), equalTo(Collections.singletonMap("already_in_upgrade_mode", true)));
            verifyNoMoreInteractions(client);

            transformPlugin.indicesMigrationComplete(
                response.get(),
                client,
                ActionTestUtils.assertNoFailureListener(ESTestCase::assertTrue)
            );

            verifyNoMoreInteractions(client);
        }
    }
}
