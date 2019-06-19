/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MergeableClusterPrivilegeTests extends ESTestCase {

    private User user;
    private Authentication authentication = mock(Authentication.class);
    private Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);

    @Before
    public void setup() {
        user = new User("user1");
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getName()).thenReturn("realm1");
        when(authenticatedBy.getType()).thenReturn("kerberos");
    }

    public void testMergeNonConditionalClusterPrivilegesOnly() {
        final List<ClusterPrivilege> clusterPrivileges = List.of(ClusterPrivilegeResolver.resolve(Set.of("manage_token")),
                ClusterPrivilegeResolver.resolve(Set.of("manage_api_key")));
        final ClusterPrivilege cp = MergeableClusterPrivilege.merge(clusterPrivileges);
        assertThat(cp, instanceOf(ClusterPrivilege.class));
        assertThat(cp, not(instanceOf(MergeableClusterPrivilege.class)));
        assertThat(cp.predicate().test("cluster:admin/xpack/security/token/invalidate"), is(true));
        assertThat(cp.predicate().test("cluster:admin/xpack/security/api_key/invalidate"), is(true));
    }

    public void testMergeConditionalClusterPrivilegesOnly() {
        final List<ClusterPrivilege> clusterPrivileges = List.of(ClusterPrivilegeResolver.resolve(Set.of("manage_own_api_key")),
                new ManageApplicationPrivileges(Set.of("kibana-app-*")));
        final ClusterPrivilege cp = MergeableClusterPrivilege.merge(clusterPrivileges);
        assertThat(cp, instanceOf(ClusterPrivilege.class));
        assertThat(cp, instanceOf(MergeableClusterPrivilege.class));
        ConditionalClusterPrivilege ccp = (ConditionalClusterPrivilege) cp;
        assertThat(ccp.predicate().test("cluster:admin/xpack/security/token/invalidate"), is(false));
        assertThat(ccp.predicate().test("cluster:admin/xpack/security/api_key/invalidate"), is(true));

        TransportRequest tr = new InvalidateTokenRequest();
        assertThat(ccp.getRequestPredicate().test(tr, authentication), is(false));
        tr = InvalidateApiKeyRequest.usingApiKeyId("user1-api-key-id");
        // API key id is always required to evaluate condition if authenticated by API key id
        when(authenticatedBy.getName()).thenReturn("_es_api_key");
        when(authenticatedBy.getType()).thenReturn("_es_api_key");
        when(authentication.getMetadata()).thenReturn(Map.of("_security_api_key_id", "user1-api-key-id"));
        assertThat(ccp.getRequestPredicate().test(tr, authentication), is(true));

        PutPrivilegesRequest putPrivilegesReq = new PutPrivilegesRequest();
        putPrivilegesReq.setPrivileges(
                List.of(new ApplicationPrivilegeDescriptor("kibana-app-1", "foo", Set.of("read", "write"), Collections.emptyMap())));
        assertThat(ccp.predicate().test("cluster:admin/xpack/security/privilege/put"), is(true));
        assertThat(ccp.getRequestPredicate().test(putPrivilegesReq, authentication), is(true));
    }

    public void testMergeMixOfConditionalAndNonConditionalClusterPrivileges() {
        final List<ClusterPrivilege> clusterPrivileges = List.of(ClusterPrivilegeResolver.resolve(Set.of("manage_token")),
                ClusterPrivilegeResolver.resolve(Set.of("manage_own_api_key")));
        final ClusterPrivilege cp = MergeableClusterPrivilege.merge(clusterPrivileges);
        assertThat(cp, instanceOf(ClusterPrivilege.class));
        assertThat(cp, instanceOf(MergeableClusterPrivilege.class));
        ConditionalClusterPrivilege ccp = (ConditionalClusterPrivilege) cp;
        assertThat(ccp.predicate().test("cluster:admin/xpack/security/token/invalidate"), is(true));
        assertThat(ccp.predicate().test("cluster:admin/xpack/security/api_key/invalidate"), is(true));

        TransportRequest tr = new InvalidateTokenRequest();
        assertThat(ccp.getRequestPredicate().test(tr, authentication), is(false));
        tr = InvalidateApiKeyRequest.usingApiKeyId("user1-api-key-id");
        // API key id is always required to evaluate condition if authenticated by API key id
        when(authenticatedBy.getName()).thenReturn("_es_api_key");
        when(authenticatedBy.getType()).thenReturn("_es_api_key");
        when(authentication.getMetadata()).thenReturn(Map.of("_security_api_key_id", "user1-api-key-id"));
        assertThat(ccp.getRequestPredicate().test(tr, authentication), is(true));
    }
}
