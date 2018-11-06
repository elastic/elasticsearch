/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivileges.ManageApplicationPrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.mockito.Mockito.mock;

public class TransportGetUserPrivilegesActionTests extends ESTestCase {

    public void testBuildResponseObject() {
        final ManageApplicationPrivileges manageApplicationPrivileges = new ManageApplicationPrivileges(Sets.newHashSet("app01", "app02"));
        final BytesArray query = new BytesArray("{\"term\":{\"public\":true}}");
        final Role role = Role.builder("test", "role")
            .cluster(Sets.newHashSet("monitor", "manage_watcher"), Collections.singleton(manageApplicationPrivileges))
            .add(IndexPrivilege.get(Sets.newHashSet("read", "write")), "index-1")
            .add(IndexPrivilege.ALL, "index-2", "index-3")
            .add(
                new FieldPermissions(new FieldPermissionsDefinition(new String[]{ "public.*" }, new String[0])),
                Collections.singleton(query),
                IndexPrivilege.READ, "index-4", "index-5")
            .addApplicationPrivilege(new ApplicationPrivilege("app01", "read", "data:read"), Collections.singleton("*"))
            .runAs(new Privilege(Sets.newHashSet("user01", "user02"), "user01", "user02"))
            .build();

        final TransportGetUserPrivilegesAction action = new TransportGetUserPrivilegesAction(mock(ThreadPool.class),
            mock(TransportService.class), mock(ActionFilters.class), mock(AuthorizationService.class));
        final GetUserPrivilegesResponse response = action.buildResponseObject(role);

        assertThat(response.getClusterPrivileges(), containsInAnyOrder("monitor", "manage_watcher"));
        assertThat(response.getConditionalClusterPrivileges(), containsInAnyOrder(manageApplicationPrivileges));

        assertThat(response.getIndexPrivileges(), iterableWithSize(3));
        final GetUserPrivilegesResponse.Indices index1 = findIndexPrivilege(response.getIndexPrivileges(), "index-1");
        assertThat(index1.getIndices(), containsInAnyOrder("index-1"));
        assertThat(index1.getPrivileges(), containsInAnyOrder("read", "write"));
        assertThat(index1.getFieldSecurity(), emptyIterable());
        assertThat(index1.getQueries(), emptyIterable());
        final GetUserPrivilegesResponse.Indices index2 = findIndexPrivilege(response.getIndexPrivileges(), "index-2");
        assertThat(index2.getIndices(), containsInAnyOrder("index-2", "index-3"));
        assertThat(index2.getPrivileges(), containsInAnyOrder("all"));
        assertThat(index2.getFieldSecurity(), emptyIterable());
        assertThat(index2.getQueries(), emptyIterable());
        final GetUserPrivilegesResponse.Indices index4 = findIndexPrivilege(response.getIndexPrivileges(), "index-4");
        assertThat(index4.getIndices(), containsInAnyOrder("index-4", "index-5"));
        assertThat(index4.getPrivileges(), containsInAnyOrder("read"));
        assertThat(index4.getFieldSecurity(), containsInAnyOrder(
            new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[]{ "public.*" }, new String[0])));
        assertThat(index4.getQueries(), containsInAnyOrder(query));

        assertThat(response.getApplicationPrivileges(), containsInAnyOrder(
            RoleDescriptor.ApplicationResourcePrivileges.builder().application("app01").privileges("read").resources("*").build())
        );

        assertThat(response.getRunAs(), containsInAnyOrder("user01", "user02"));
    }

    private GetUserPrivilegesResponse.Indices findIndexPrivilege(Set<GetUserPrivilegesResponse.Indices> indices, String name) {
        return indices.stream().filter(i -> i.getIndices().contains(name)).findFirst().get();
    }
}
