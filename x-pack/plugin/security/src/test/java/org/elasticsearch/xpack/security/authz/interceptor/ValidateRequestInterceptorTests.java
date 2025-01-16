/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Set;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.security.Security.DLS_ERROR_WHEN_VALIDATE_QUERY_WITH_REWRITE;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ValidateRequestInterceptorTests extends ESTestCase {

    private ThreadPool threadPool;
    private MockLicenseState licenseState;
    private ValidateRequestInterceptor interceptor;
    private Settings settings;

    @Before
    public void init() {
        threadPool = new TestThreadPool("validate request interceptor tests");
        licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        settings = Settings.EMPTY;
        interceptor = new ValidateRequestInterceptor(threadPool, licenseState, settings);
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testValidateRequestWithDLS() {
        final DocumentPermissions documentPermissions = DocumentPermissions.filteredBy(
            Set.of(new BytesArray("{\"term\":{\"username\":\"foo\"}}"))
        ); // value does not matter
        ElasticsearchClient client = mock(ElasticsearchClient.class);
        ValidateQueryRequestBuilder builder = new ValidateQueryRequestBuilder(client, ValidateQueryAction.INSTANCE);
        final String index = randomAlphaOfLengthBetween(3, 8);
        final PlainActionFuture<Void> listener1 = new PlainActionFuture<>();
        Map<String, IndicesAccessControl.IndexAccessControl> accessControlMap = Collections.singletonMap(
            index,
            new IndicesAccessControl.IndexAccessControl(false, FieldPermissions.DEFAULT, documentPermissions)
        );
        // with DLS and rewrite enabled
        interceptor.disableFeatures(builder.setRewrite(true).request(), accessControlMap, listener1);
        ElasticsearchSecurityException exception = expectThrows(ElasticsearchSecurityException.class, () -> listener1.actionGet());
        assertThat(exception.getMessage(), containsString("Validate with rewrite isn't supported if document level security is enabled"));

        // with DLS and rewrite disabled
        final PlainActionFuture<Void> listener2 = new PlainActionFuture<>();
        interceptor.disableFeatures(builder.setRewrite(false).request(), accessControlMap, listener2);
        assertNull(listener2.actionGet());

    }

    public void testValidateRequestWithOutDLS() {
        final DocumentPermissions documentPermissions = null; // no DLS
        ElasticsearchClient client = mock(ElasticsearchClient.class);
        ValidateQueryRequestBuilder builder = new ValidateQueryRequestBuilder(client, ValidateQueryAction.INSTANCE);
        final String index = randomAlphaOfLengthBetween(3, 8);
        final PlainActionFuture<Void> listener1 = new PlainActionFuture<>();
        Map<String, IndicesAccessControl.IndexAccessControl> accessControlMap = Collections.singletonMap(
            index,
            new IndicesAccessControl.IndexAccessControl(false, FieldPermissions.DEFAULT, documentPermissions)
        );
        // without DLS and rewrite enabled
        interceptor.disableFeatures(builder.setRewrite(true).request(), accessControlMap, listener1);
        assertNull(listener1.actionGet());

        // without DLS and rewrite disabled
        final PlainActionFuture<Void> listener2 = new PlainActionFuture<>();
        interceptor.disableFeatures(builder.setRewrite(false).request(), accessControlMap, listener2);
        assertNull(listener2.actionGet());
    }

    public void testValidateRequestWithDLSConfig() {
        ValidateQueryRequest request = mock(ValidateQueryRequest.class);
        when(request.rewrite()).thenReturn(true);
        // default
        assertTrue(interceptor.supports(request));

        // explicit configuration - same as default
        settings = Settings.builder()
            .put(DLS_ERROR_WHEN_VALIDATE_QUERY_WITH_REWRITE.getKey(), DLS_ERROR_WHEN_VALIDATE_QUERY_WITH_REWRITE.getDefault(Settings.EMPTY))
            .build();
        interceptor = new ValidateRequestInterceptor(threadPool, licenseState, settings);
        assertTrue(interceptor.supports(request));
        assertWarnings(
            "[xpack.security.dls.error_when_validate_query_with_rewrite.enabled] setting was deprecated in Elasticsearch and will be "
                + "removed in a future release! See the breaking changes documentation for the next major version."
        );

        // explicit configuration - opposite of default
        settings = Settings.builder()
            .put(
                DLS_ERROR_WHEN_VALIDATE_QUERY_WITH_REWRITE.getKey(),
                DLS_ERROR_WHEN_VALIDATE_QUERY_WITH_REWRITE.getDefault(Settings.EMPTY) == false
            )
            .build();
        interceptor = new ValidateRequestInterceptor(threadPool, licenseState, settings);
        assertFalse(interceptor.supports(request));
        assertWarnings(
            "[xpack.security.dls.error_when_validate_query_with_rewrite.enabled] setting was deprecated in Elasticsearch and will be "
                + "removed in a future release! See the breaking changes documentation for the next major version."
        );
    }
}
