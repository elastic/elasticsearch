/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AuthorizationStatePersistenceUtilsTests extends TransformSingleNodeTestCase {

    private ClusterService clusterService;
    private IndexBasedTransformConfigManager transformConfigManager;

    @Before
    public void createComponents() {
        clusterService = mock(ClusterService.class);
        transformConfigManager = new IndexBasedTransformConfigManager(
            clusterService,
            TestIndexNameExpressionResolver.newInstance(),
            client(),
            xContentRegistry()
        );
    }

    public void testAuthState() throws InterruptedException {
        String transformId = "transform_test_auth_state_create_read_update";

        assertAsync(
            listener -> AuthorizationStatePersistenceUtils.fetchAuthState(transformConfigManager, transformId, listener),
            (AuthorizationState) null
        );

        AuthorizationState greenAuthState = AuthorizationState.green();
        assertAsync(
            listener -> AuthorizationStatePersistenceUtils.persistAuthState(
                Settings.EMPTY,
                transformConfigManager,
                transformId,
                greenAuthState,
                listener
            ),
            (Void) null
        );

        assertAsync(
            listener -> AuthorizationStatePersistenceUtils.fetchAuthState(transformConfigManager, transformId, listener),
            greenAuthState
        );

        AuthorizationState redAuthState = AuthorizationState.red(new ElasticsearchSecurityException("missing privileges"));
        assertAsync(
            listener -> AuthorizationStatePersistenceUtils.persistAuthState(
                Settings.EMPTY,
                transformConfigManager,
                transformId,
                redAuthState,
                listener
            ),
            (Void) null
        );

        assertAsync(
            listener -> AuthorizationStatePersistenceUtils.fetchAuthState(transformConfigManager, transformId, listener),
            redAuthState
        );

        AuthorizationState otherRedAuthState = AuthorizationState.red(new ElasticsearchSecurityException("other missing privileges"));
        assertAsync(
            listener -> AuthorizationStatePersistenceUtils.persistAuthState(
                Settings.EMPTY,
                transformConfigManager,
                transformId,
                otherRedAuthState,
                listener
            ),
            (Void) null
        );

        assertAsync(
            listener -> AuthorizationStatePersistenceUtils.fetchAuthState(transformConfigManager, transformId, listener),
            otherRedAuthState
        );
    }

    private <T> void assertAsync(Consumer<ActionListener<T>> function, T expected) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<T> listener = new LatchedActionListener<>(
            ActionTestUtils.assertNoFailureListener(r -> assertThat(r, is(equalTo(expected)))),
            latch
        );
        function.accept(listener);
        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
    }
}
