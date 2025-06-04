/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ValidateRequestInterceptorTests extends ESTestCase {

    private ThreadPool threadPool;
    private MockLicenseState licenseState;
    private ValidateRequestInterceptor interceptor;

    @Before
    public void init() {
        threadPool = new TestThreadPool("validate request interceptor tests");
        licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        interceptor = new ValidateRequestInterceptor(threadPool, licenseState);
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testValidateRequestWithDLS() {
        final DocumentPermissions documentPermissions = DocumentPermissions.filteredBy(Set.of(new BytesArray("""
            {"term":{"username":"foo"}}"""))); // value does not matter
        ElasticsearchClient client = mock(ElasticsearchClient.class);
        ValidateQueryRequestBuilder builder = new ValidateQueryRequestBuilder(client);
        final String index = randomAlphaOfLengthBetween(3, 8);
        final PlainActionFuture<Void> listener1 = new PlainActionFuture<>();
        Map<String, IndicesAccessControl.IndexAccessControl> accessControlMap = Map.of(
            index,
            new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, documentPermissions)
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
        ValidateQueryRequestBuilder builder = new ValidateQueryRequestBuilder(client);
        final String index = randomAlphaOfLengthBetween(3, 8);
        final PlainActionFuture<Void> listener1 = new PlainActionFuture<>();
        Map<String, IndicesAccessControl.IndexAccessControl> accessControlMap = Map.of(
            index,
            new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, documentPermissions)
        );
        // without DLS and rewrite enabled
        interceptor.disableFeatures(builder.setRewrite(true).request(), accessControlMap, listener1);
        assertNull(listener1.actionGet());

        // without DLS and rewrite disabled
        final PlainActionFuture<Void> listener2 = new PlainActionFuture<>();
        interceptor.disableFeatures(builder.setRewrite(false).request(), accessControlMap, listener2);
        assertNull(listener2.actionGet());
    }
}
