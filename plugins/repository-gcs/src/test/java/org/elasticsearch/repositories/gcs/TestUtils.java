/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Random;
import java.util.UUID;

final class TestUtils {

    private TestUtils() {}

    /**
     * Creates a random Service Account file for testing purpose
     */
    static byte[] createServiceAccount(final Random random) {
        try {
            final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
            keyPairGenerator.initialize(2048);
            final String privateKey = Base64.getEncoder().encodeToString(keyPairGenerator.generateKeyPair().getPrivate().getEncoded());

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), out)) {
                builder.startObject();
                {
                    builder.field("type", "service_account");
                    builder.field("project_id", "test");
                    builder.field("private_key_id", UUID.randomUUID().toString());
                    builder.field("private_key", "-----BEGIN PRIVATE KEY-----\n" + privateKey + "\n-----END PRIVATE KEY-----\n");
                    builder.field("client_email", "elastic@appspot.gserviceaccount.com");
                    builder.field("client_id", String.valueOf(Math.abs(random.nextLong())));
                }
                builder.endObject();
            }
            return out.toByteArray();
        } catch (Exception e) {
            throw new AssertionError("Unable to create service account file", e);
        }
    }
}
