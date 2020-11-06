/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.role;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilder;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;

public class PutRoleBuilderTests extends ESTestCase {
    // test that we reject a role where field permissions are stored in 2.x format (fields:...)
    public void testBWCFieldPermissions() throws Exception {
        Path path = getDataPath("roles2xformat.json");
        byte[] bytes = Files.readAllBytes(path);
        String roleString = new String(bytes, Charset.defaultCharset());
        try (Client client = new NoOpClient("testBWCFieldPermissions")) {
            ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
                    () -> new PutRoleRequestBuilder(client).source("role1", new BytesArray(roleString), XContentType.JSON));
            assertThat(e.getDetailedMessage(), containsString("\"fields\": [...]] format has changed for field permissions in role " +
                    "[role1], use [\"field_security\": {\"grant\":[...],\"except\":[...]}] instead"));
        }
    }
}
