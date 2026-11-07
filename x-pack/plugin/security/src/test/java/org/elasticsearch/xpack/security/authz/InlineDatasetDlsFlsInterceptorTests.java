/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.security.authz.RBACEngine.RBACAuthorizationInfo;
import org.elasticsearch.xpack.security.authz.interceptor.ViewAndDatasetDlsFlsRequestInterceptor;
import org.junit.Before;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

/**
 * The arm of {@link ViewAndDatasetDlsFlsRequestInterceptor} that refuses a read of a dataset the query defined
 * rather than named. Such a read is authorized under the data source's name, which the engine usually resolves to
 * nothing — and then the resolved access control records no restriction for it however the role is written, so the
 * role itself has to be asked.
 * <p>
 * The registered-dataset arm is covered in that class's own test, in the interceptor's package. These cases live
 * here because they need {@link RBACAuthorizationInfo}, which that package cannot build, and because they are about
 * the role rather than the access control. Each is a shape where asking the role the cheap way — one indices
 * permission, matched with {@code Regex} — let a restricted read through: an intersection of grants, which has no
 * single permission to walk, and a grant spelled with a wildcard the engine honours and {@code Regex} does not.
 */
public class InlineDatasetDlsFlsInterceptorTests extends ESTestCase {

    private static final String DATA_SOURCE = "ds1";
    private static final String REFUSAL = "Datasets with document or field level security restrictions are not supported.";

    private ThreadContext threadContext;
    private ViewAndDatasetDlsFlsRequestInterceptor interceptor;

    @Before
    public void setUpInterceptor() {
        threadContext = new ThreadContext(Settings.EMPTY);
        interceptor = new ViewAndDatasetDlsFlsRequestInterceptor(threadContext, () -> mock(ProjectMetadata.class));
    }

    public void testADocumentLevelGrantOnTheNameIsRefused() {
        assertRefused(roleWith(dlsQuery(), FieldPermissions.DEFAULT, DATA_SOURCE));
    }

    public void testAFieldLevelGrantOnTheNameIsRefused() {
        assertRefused(roleWith(null, restrictedFields(), DATA_SOURCE));
    }

    public void testAnUnrestrictedGrantIsLetThrough() {
        assertAllowed(roleWith(null, FieldPermissions.DEFAULT, DATA_SOURCE));
    }

    public void testARestrictionOnAnotherNameIsLetThrough() {
        assertAllowed(roleWith(dlsQuery(), FieldPermissions.DEFAULT, "something_else"));
    }

    /**
     * {@code ?} is a wildcard to the matcher authorization resolves grants with, and a literal to
     * {@code Regex.simpleMatch}. A gate using the latter grants the read and then fails to see the restriction on it.
     */
    public void testASingleCharacterWildcardGrantIsRefused() {
        assertRefused(roleWith(dlsQuery(), FieldPermissions.DEFAULT, "d?1"));
    }

    public void testATrailingWildcardGrantIsRefused() {
        assertRefused(roleWith(dlsQuery(), FieldPermissions.DEFAULT, "ds*"));
    }

    /**
     * An API key carrying role descriptors, and a cloud-capped user, both authorize under an intersection of grants.
     * It has no single indices permission to walk — asking one for it throws — and either side restricting is enough.
     */
    public void testAnIntersectionOfGrantsIsAnsweredFromBothSides() {
        Role open = roleWith(null, FieldPermissions.DEFAULT, DATA_SOURCE);
        Role closed = roleWith(dlsQuery(), FieldPermissions.DEFAULT, DATA_SOURCE);

        assertAllowed(open.limitedBy(roleWith(null, FieldPermissions.DEFAULT, DATA_SOURCE)));
        assertRefused(open.limitedBy(closed));
        assertRefused(closed.limitedBy(open));
    }

    /** A restriction the engine did record is enough on its own: a data source may be named after a real index. */
    public void testARestrictionInTheAccessControlIsEnough() {
        new SecurityContext(Settings.EMPTY, threadContext).putIndicesAccessControl(
            new IndicesAccessControl(
                true,
                Map.of(DATA_SOURCE, new IndicesAccessControl.IndexAccessControl(restrictedFields(), DocumentPermissions.allowAll()))
            )
        );

        assertRefused(roleWith(null, FieldPermissions.DEFAULT, DATA_SOURCE));
    }

    /** Neither source can answer, so the read is refused rather than served unfiltered. */
    public void testACallerThatCannotBeInterrogatedIsRefused() {
        assertRefused(mock(AuthorizationInfo.class));
    }

    /** Another action's request is none of this arm's business. */
    public void testAnotherActionIsUntouched() {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        RequestInfo other = new RequestInfo(
            AuthenticationTestHelper.builder().build(),
            new InlineReadRequest(),
            "indices:data/read/esql",
            null
        );
        interceptor.intercept(other, null, mock(AuthorizationInfo.class)).addListener(future);

        assertNull(future.actionGet());
    }

    private void assertAllowed(Role role) {
        intercept(new RBACAuthorizationInfo(role, role)).actionGet();
    }

    private void assertRefused(Role role) {
        assertRefused(new RBACAuthorizationInfo(role, role));
    }

    private void assertRefused(AuthorizationInfo authorizationInfo) {
        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> intercept(authorizationInfo).actionGet()
        );
        assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), containsString(REFUSAL));
    }

    private PlainActionFuture<Void> intercept(AuthorizationInfo authorizationInfo) {
        RequestInfo requestInfo = new RequestInfo(
            AuthenticationTestHelper.builder().build(),
            new InlineReadRequest(),
            EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASOURCE_ACTION_NAME,
            null
        );
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        interceptor.intercept(requestInfo, null, authorizationInfo).addListener(future);
        return future;
    }

    private static Set<BytesReference> dlsQuery() {
        return Set.of(new BytesArray("{\"match_all\":{}}"));
    }

    private static FieldPermissions restrictedFields() {
        return new FieldPermissions(new FieldPermissionsDefinition(new String[] { "allowed" }, null));
    }

    private static Role roleWith(Set<BytesReference> query, FieldPermissions fieldPermissions, String... indices) {
        return Role.builder(new RestrictedIndices(Automatons.EMPTY), "test")
            .add(fieldPermissions, query, IndexPrivilege.READ, false, indices)
            .build();
    }

    /** The shape {@code EsqlAuthorizeDatasourceAction.Request} has, without depending on the ES|QL plugin. */
    private static final class InlineReadRequest extends ActionRequest implements IndicesRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String[] indices() {
            return new String[] { DATA_SOURCE };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.DEFAULT;
        }
    }
}
