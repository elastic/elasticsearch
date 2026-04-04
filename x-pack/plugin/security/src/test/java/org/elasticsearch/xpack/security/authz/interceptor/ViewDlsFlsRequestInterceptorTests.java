/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ViewDlsFlsRequestInterceptorTests extends ESTestCase {

    private ThreadContext threadContext;
    private Authentication authentication;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(Settings.EMPTY);
        authentication = AuthenticationTestHelper.builder()
            .user(new User("test-user", "test-role"))
            .realmRef(new Authentication.RealmRef("realm", "type", "node", null))
            .build();
    }

    public void testRejectsViewWithDls() {
        String viewName = "test-view";
        ProjectMetadata projectMetadata = mockProjectMetadata(viewName);
        ViewDlsFlsRequestInterceptor interceptor = new ViewDlsFlsRequestInterceptor(threadContext, () -> projectMetadata);

        DocumentPermissions docPerms = DocumentPermissions.filteredBy(Set.of(new BytesArray("{\"terms\" : { \"tk1\" : [\"tv1\"] } }")));
        setAccessControl(Map.of(viewName, new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, docPerms)));

        RequestInfo requestInfo = new RequestInfo(authentication, buildIndicesRequest(viewName), "indices:data/read/esql", null);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        interceptor.intercept(requestInfo, null, null).addListener(future);
        ElasticsearchSecurityException ex = expectThrows(ElasticsearchSecurityException.class, future::actionGet);

        validateException(ex, viewName);
    }

    public void testRejectsViewWithFls() {
        String viewName = "test-view";
        ProjectMetadata projectMetadata = mockProjectMetadata(viewName);
        ViewDlsFlsRequestInterceptor interceptor = new ViewDlsFlsRequestInterceptor(threadContext, () -> projectMetadata);

        FieldPermissions fieldPerms = new FieldPermissions(new FieldPermissionsDefinition(new String[] { "value" }, null));
        setAccessControl(Map.of(viewName, new IndicesAccessControl.IndexAccessControl(fieldPerms, DocumentPermissions.allowAll())));

        RequestInfo requestInfo = new RequestInfo(authentication, buildIndicesRequest(viewName), "indices:data/read/esql", null);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        interceptor.intercept(requestInfo, null, null).addListener(future);
        ElasticsearchSecurityException ex = expectThrows(ElasticsearchSecurityException.class, future::actionGet);

        validateException(ex, viewName);
    }

    public void testRejectsViewWithBothDlsAndFls() {
        String viewName = "test-view";
        ProjectMetadata projectMetadata = mockProjectMetadata(viewName);
        ViewDlsFlsRequestInterceptor interceptor = new ViewDlsFlsRequestInterceptor(threadContext, () -> projectMetadata);

        FieldPermissions fieldPerms = new FieldPermissions(new FieldPermissionsDefinition(new String[] { "value" }, null));
        DocumentPermissions docPerms = DocumentPermissions.filteredBy(Set.of(new BytesArray("{\"terms\" : { \"tk1\" : [\"tv1\"] } }")));
        setAccessControl(Map.of(viewName, new IndicesAccessControl.IndexAccessControl(fieldPerms, docPerms)));

        RequestInfo requestInfo = new RequestInfo(authentication, buildIndicesRequest(viewName), "indices:data/read/esql", null);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        interceptor.intercept(requestInfo, null, null).addListener(future);
        ElasticsearchSecurityException ex = expectThrows(ElasticsearchSecurityException.class, future::actionGet);

        validateException(ex, viewName);
    }

    public void testAllowsViewWithoutDlsOrFls() {
        String viewName = "test-view";
        ProjectMetadata projectMetadata = mockProjectMetadata(viewName);
        ViewDlsFlsRequestInterceptor interceptor = new ViewDlsFlsRequestInterceptor(threadContext, () -> projectMetadata);

        setAccessControl(
            Map.of(viewName, new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, DocumentPermissions.allowAll()))
        );

        validateNoException(interceptor, buildIndicesRequest(viewName));
    }

    public void testAllowsNonViewIndexWithDls() {
        String indexName = "regular-index";
        ProjectMetadata projectMetadata = mockProjectMetadata();
        ViewDlsFlsRequestInterceptor interceptor = new ViewDlsFlsRequestInterceptor(threadContext, () -> projectMetadata);

        DocumentPermissions docPerms = DocumentPermissions.filteredBy(Set.of(new BytesArray("{\"terms\" : { \"tk1\" : [\"tv1\"] } }")));
        setAccessControl(Map.of(indexName, new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, docPerms)));

        validateNoException(interceptor, buildIndicesRequest(indexName));
    }

    public void testIgnoresNonIndexRequest() {
        String viewName = "test-view";
        ProjectMetadata projectMetadata = mockProjectMetadata(viewName);
        ViewDlsFlsRequestInterceptor interceptor = new ViewDlsFlsRequestInterceptor(threadContext, () -> projectMetadata);

        DocumentPermissions docPerms = DocumentPermissions.filteredBy(Set.of(new BytesArray("{\"terms\" : { \"tk1\" : [\"tv1\"] } }")));
        setAccessControl(Map.of(viewName, new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, docPerms)));
        TransportRequest request = mock(TransportRequest.class);

        validateNoException(interceptor, request);
    }

    public void testIgnoresRequestWithNoResolveViews() {
        String viewName = "test-view";
        ProjectMetadata projectMetadata = mockProjectMetadata(viewName);
        ViewDlsFlsRequestInterceptor interceptor = new ViewDlsFlsRequestInterceptor(threadContext, () -> projectMetadata);

        DocumentPermissions docPerms = DocumentPermissions.filteredBy(Set.of(new BytesArray("{\"terms\" : { \"tk1\" : [\"tv1\"] } }")));
        setAccessControl(Map.of(viewName, new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, docPerms)));
        TestIndicesRequest request = buildIndicesRequest(viewName);

        // override the indices options to disable resolve views, which should cause the interceptor to skip the DLS/FLS checks
        doReturn(IndicesOptions.builder().build()).when(request).indicesOptions();

        validateNoException(interceptor, request);
    }

    public void testMetadataContainsMultipleViewNames() {
        String view1 = "view-a";
        String view2 = "view-b";
        ProjectMetadata projectMetadata = mock(ProjectMetadata.class);
        when(projectMetadata.hasView(view1)).thenReturn(true);
        when(projectMetadata.hasView(view2)).thenReturn(true);
        ViewDlsFlsRequestInterceptor interceptor = new ViewDlsFlsRequestInterceptor(threadContext, () -> projectMetadata);

        DocumentPermissions docPerms = DocumentPermissions.filteredBy(Set.of(new BytesArray("{\"terms\" : { \"tk1\" : [\"tv1\"] } }")));
        setAccessControl(
            Map.of(
                view1,
                new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, docPerms),
                view2,
                new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, docPerms)
            )
        );

        RequestInfo requestInfo = new RequestInfo(authentication, buildIndicesRequest(view1, view2), "indices:data/read/esql", null);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        interceptor.intercept(requestInfo, null, null).addListener(future);
        ElasticsearchSecurityException ex = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        validateException(ex, view1, view2);
    }

    public void testMixedViewsAndIndicesOnlyReportsViews() {
        String viewName = "test-view";
        String indexName = "regular-index";
        ProjectMetadata projectMetadata = mock(ProjectMetadata.class);
        when(projectMetadata.hasView(viewName)).thenReturn(true);
        when(projectMetadata.hasView(indexName)).thenReturn(false);
        ViewDlsFlsRequestInterceptor interceptor = new ViewDlsFlsRequestInterceptor(threadContext, () -> projectMetadata);

        DocumentPermissions docPerms = DocumentPermissions.filteredBy(Set.of(new BytesArray("{\"terms\" : { \"tk1\" : [\"tv1\"] } }")));
        setAccessControl(
            Map.of(
                viewName,
                new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, docPerms),
                indexName,
                new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, docPerms)
            )
        );

        RequestInfo requestInfo = new RequestInfo(authentication, buildIndicesRequest(viewName, indexName), "indices:data/read/esql", null);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        interceptor.intercept(requestInfo, null, null).addListener(future);
        ElasticsearchSecurityException ex = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        validateException(ex, viewName);
    }

    private interface TestIndicesRequest extends IndicesRequest, TransportRequest {}

    private TestIndicesRequest buildIndicesRequest(String... indices) {
        TestIndicesRequest request = mock(TestIndicesRequest.class);
        when(request.indices()).thenReturn(indices);
        when(request.indicesOptions()).thenReturn(
            IndicesOptions.builder()
                .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveViews(true).build())
                .build()
        );
        return request;
    }

    private void validateNoException(ViewDlsFlsRequestInterceptor interceptor, TransportRequest request) {
        RequestInfo requestInfo = new RequestInfo(authentication, request, "indices:data/read/esql", null);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        interceptor.intercept(requestInfo, null, null).addListener(future);
        future.actionGet();
    }

    private void validateException(ElasticsearchSecurityException ex, String... expectedViewNames) {
        assertThat(ex.status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(
            ex.getMessage(),
            equalTo(
                "Views with document or field level security restrictions are not supported."
                    + " Remove DLS/FLS restrictions from the affected views in the role definition, or exclude the views from the request."
            )
        );
        List<String> metadata = ex.getMetadata("es.views_with_dls_or_fls");
        assertThat(metadata, containsInAnyOrder(expectedViewNames));
    }

    private void setAccessControl(Map<String, IndicesAccessControl.IndexAccessControl> indexPermissions) {
        IndicesAccessControl accessControl = new IndicesAccessControl(true, indexPermissions);
        new SecurityContext(Settings.EMPTY, threadContext).putIndicesAccessControl(accessControl);
    }

    private static ProjectMetadata mockProjectMetadata(String... viewNames) {
        ProjectMetadata projectMetadata = mock(ProjectMetadata.class);
        for (String viewName : viewNames) {
            when(projectMetadata.hasView(viewName)).thenReturn(true);
        }
        return projectMetadata;
    }
}
