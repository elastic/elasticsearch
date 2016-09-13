/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

public class NativeRolesStoreTests extends ESTestCase {

    // test that we can read a role where field permissions are stored in 2.x format (fields:...)
    public void testBWCFieldPermissions() throws IOException {
        Path path = getDataPath("roles2xformat.json");
        byte[] bytes = Files.readAllBytes(path);
        String roleString = new String(bytes, Charset.defaultCharset());
        RoleDescriptor role = NativeRolesStore.transformRole("role1", new BytesArray(roleString), logger);
        RoleDescriptor.IndicesPrivileges indicesPrivileges = role.getIndicesPrivileges()[0];
        assertTrue(indicesPrivileges.getFieldPermissions().grantsAccessTo("foo"));
        assertTrue(indicesPrivileges.getFieldPermissions().grantsAccessTo("boo"));
    }
}
