/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ingest;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.CharArrays;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.ingest.HashProcessor.Method;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class HashProcessorTests extends ESTestCase {
    public void testProcessorSingleField() throws Exception {
        List<String> fields = Collections.singletonList(randomAlphaOfLength(6));
        Map<String, Object> docFields = new HashMap<>();
        for (String field : fields) {
            docFields.put(field, randomAlphaOfLengthBetween(2, 10));
        }

        String targetField = randomAlphaOfLength(6);
        Method method = randomFrom(Method.values());
        Mac mac = createMac(method);
        byte[] salt = CharArrays.toUtf8Bytes(Hasher.SaltProvider.salt(5));
        HashProcessor processor = new HashProcessor("_tag", fields, targetField, salt, method, mac);
        IngestDocument ingestDocument = new IngestDocument(docFields, new HashMap<>());
        processor.execute(ingestDocument);

        String targetFieldValue = ingestDocument.getFieldValue(targetField, String.class);
        Object expectedTargetFieldValue = method.hash(mac, salt, ingestDocument.getFieldValue(fields.get(0), String.class));
        assertThat(targetFieldValue, equalTo(expectedTargetFieldValue));
        byte[] bytes = Base64.getDecoder().decode(targetFieldValue);
        byte[] actualSaltPrefix = new byte[salt.length];
        System.arraycopy(bytes, 0, actualSaltPrefix, 0, salt.length);
        assertArrayEquals(salt, actualSaltPrefix);
    }

    @SuppressWarnings("unchecked")
    public void testProcessorMultipleFields() throws Exception {
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(2, 10); i++) {
            fields.add(randomAlphaOfLength(5 + i));
        }
        Map<String, Object> docFields = new HashMap<>();
        for (String field : fields) {
            docFields.put(field, randomAlphaOfLengthBetween(2, 10));
        }

        String targetField = randomAlphaOfLength(6);
        Method method = randomFrom(Method.values());
        Mac mac = createMac(method);
        byte[] salt = CharArrays.toUtf8Bytes(Hasher.SaltProvider.salt(5));
        HashProcessor processor = new HashProcessor("_tag", fields, targetField, salt, method, mac);
        IngestDocument ingestDocument = new IngestDocument(docFields, new HashMap<>());
        processor.execute(ingestDocument);

        Map<String, String> targetFieldMap = ingestDocument.getFieldValue(targetField, Map.class);
        for (Map.Entry<String, String> entry : targetFieldMap.entrySet()) {
            Object expectedTargetFieldValue = method.hash(mac, salt, ingestDocument.getFieldValue(entry.getKey(), String.class));
            assertThat(entry.getValue(), equalTo(expectedTargetFieldValue));
            byte[] bytes = Base64.getDecoder().decode(entry.getValue());
            byte[] actualSaltPrefix = new byte[salt.length];
            System.arraycopy(bytes, 0, actualSaltPrefix, 0, salt.length);
            assertArrayEquals(salt, actualSaltPrefix);
        }
    }

    private Mac createMac(Method method) throws Exception {
        char[] password = randomAlphaOfLengthBetween(1, 10).toCharArray();
        byte[] salt = randomAlphaOfLength(5).getBytes(StandardCharsets.UTF_8);
        int iterations = randomIntBetween(1, 10);
        SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2With" + method.getAlgorithm());
        PBEKeySpec keySpec = new PBEKeySpec(password, salt, iterations, 128);
        byte[] pbkdf2 = secretKeyFactory.generateSecret(keySpec).getEncoded();
        Mac mac = Mac.getInstance(method.getAlgorithm());
        mac.init(new SecretKeySpec(pbkdf2, method.getAlgorithm()));
        return mac;
    }
}
