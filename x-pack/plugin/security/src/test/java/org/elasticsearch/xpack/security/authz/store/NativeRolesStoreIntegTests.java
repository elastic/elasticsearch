/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheAction;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheRequest;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheResponse;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyMap;

public class NativeRolesStoreIntegTests extends SecurityIntegTestCase {

    @Before
    public void configureApplicationPrivileges() {
        final List<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors = Arrays.asList(
            new ApplicationPrivilegeDescriptor("app-1", "read", Set.of("a:b:c", "x:y:z"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app-1", "write", Set.of("a:b:c", "x:y:z"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app-1", "admin", Set.of("a:b:c", "x:y:z"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app-2", "read", Set.of("e:f:g", "t:u:v"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app-2", "write", Set.of("e:f:g", "t:u:v"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app-2", "admin", Set.of("e:f:g", "t:u:v"), emptyMap()));

        final PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest();
        putPrivilegesRequest.setPrivileges(applicationPrivilegeDescriptors);
        final ActionFuture<PutPrivilegesResponse> future =
            client().execute(PutPrivilegesAction.INSTANCE, putPrivilegesRequest);

        final PutPrivilegesResponse putPrivilegesResponse = future.actionGet();
        assertEquals(2, putPrivilegesResponse.created().size());
        assertEquals(6, putPrivilegesResponse.created().values().stream().mapToInt(List::size).sum());
    }

    public void testCache() {

        ApplicationPrivilegeDescriptor[] privileges = new GetPrivilegesRequestBuilder(client())
            .application("app-2").privileges("write").execute().actionGet().privileges();

        assertNotNull(privileges);
        assertEquals(1, privileges.length);
        assertEquals("app-2", privileges[0].getApplication());
        assertEquals("write", privileges[0].getName());

        // Invalidate cache
        final ClearPrivilegesCacheResponse clearPrivilegesCacheResponse =
            client().execute(ClearPrivilegesCacheAction.INSTANCE, new ClearPrivilegesCacheRequest()).actionGet();
        assertEquals(cluster().size(), clearPrivilegesCacheResponse.getNodes().size());
        assertFalse(clearPrivilegesCacheResponse.hasFailures());

        privileges = new GetPrivilegesRequestBuilder(client())
            .application("app-2").privileges("read").execute().actionGet().privileges();
        assertNotNull(privileges);
        assertEquals(1, privileges.length);
        assertEquals("app-2", privileges[0].getApplication());
        assertEquals("read", privileges[0].getName());
    }
}
