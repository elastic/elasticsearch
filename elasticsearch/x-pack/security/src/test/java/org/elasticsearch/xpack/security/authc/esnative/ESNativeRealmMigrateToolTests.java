/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.security.authc.esnative.ESNativeRealmMigrateTool;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for the {@code ESNativeRealmMigrateTool}
 */
public class ESNativeRealmMigrateToolTests extends CommandTestCase {

    @Override
    protected Command newCommand() {
        return new ESNativeRealmMigrateTool();
    }

    public void testUserJson() throws Exception {
        assertThat(ESNativeRealmMigrateTool.MigrateUserOrRoles.createUserJson(Strings.EMPTY_ARRAY, "hash".toCharArray()),
                equalTo("{\"password_hash\":\"hash\",\"roles\":[]}"));
        assertThat(ESNativeRealmMigrateTool.MigrateUserOrRoles.createUserJson(new String[]{"role1", "role2"}, "hash".toCharArray()),
                equalTo("{\"password_hash\":\"hash\",\"roles\":[\"role1\",\"role2\"]}"));
    }

    public void testRoleJson() throws Exception {
        RoleDescriptor.IndicesPrivileges ip = RoleDescriptor.IndicesPrivileges.builder()
                .indices(new String[]{"i1", "i2", "i3"})
                .privileges(new String[]{"all"})
                .fields(new String[]{"body"})
                .build();
        RoleDescriptor.IndicesPrivileges[] ips = new RoleDescriptor.IndicesPrivileges[1];
        ips[0] = ip;
        String[] cluster = Strings.EMPTY_ARRAY;
        String[] runAs = Strings.EMPTY_ARRAY;
        RoleDescriptor rd = new RoleDescriptor("rolename", cluster, ips, runAs);
        assertThat(ESNativeRealmMigrateTool.MigrateUserOrRoles.createRoleJson(rd),
                equalTo("{\"cluster\":[],\"indices\":[{\"names\":[\"i1\",\"i2\",\"i3\"]," +
                                "\"privileges\":[\"all\"],\"fields\":[\"body\"]}],\"run_as\":[]}"));

    }

}
