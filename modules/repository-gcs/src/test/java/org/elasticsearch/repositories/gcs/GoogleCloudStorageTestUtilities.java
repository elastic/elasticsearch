/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.ServiceAccountCredentials;

import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Collections;

public class GoogleCloudStorageTestUtilities {

    /** Generates a random GoogleCredential along with its corresponding Service Account file provided as a byte array **/
    public static Tuple<ServiceAccountCredentials, byte[]> randomCredential(final String clientName) throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final ServiceAccountCredentials.Builder credentialBuilder = ServiceAccountCredentials.newBuilder();
        credentialBuilder.setClientId("id_" + clientName);
        credentialBuilder.setClientEmail(clientName);
        credentialBuilder.setProjectId("project_id_" + clientName);
        credentialBuilder.setPrivateKey(keyPair.getPrivate());
        credentialBuilder.setPrivateKeyId("private_key_id_" + clientName);
        credentialBuilder.setScopes(Collections.singleton(StorageScopes.DEVSTORAGE_FULL_CONTROL));
        URI tokenServerUri = URI.create("http://localhost/oauth2/token");
        credentialBuilder.setTokenServerUri(tokenServerUri);
        final String encodedPrivateKey = Base64.getEncoder().encodeToString(keyPair.getPrivate().getEncoded());
        final String serviceAccount = Strings.format("""
            {
              "type": "service_account",
              "project_id": "project_id_%s",
              "private_key_id": "private_key_id_%s",
              "private_key": "-----BEGIN PRIVATE KEY-----\\n%s\\n-----END PRIVATE KEY-----\\n",
              "client_email": "%s",
              "client_id": "id_%s",
              "token_uri": "%s"
            }""", clientName, clientName, encodedPrivateKey, clientName, clientName, tokenServerUri);
        return Tuple.tuple(credentialBuilder.build(), serviceAccount.getBytes(StandardCharsets.UTF_8));
    }
}
