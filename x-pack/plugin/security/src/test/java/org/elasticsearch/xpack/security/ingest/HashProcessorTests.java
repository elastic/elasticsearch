/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ingest;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.ingest.HashProcessor.Method;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class HashProcessorTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testIgnoreMissing() throws Exception {
        Method method = randomFrom(Method.values());
        Mac mac = createMac(method);
        Map<String, Object> fields = new HashMap<>();
        fields.put("one", "foo");
        HashProcessor processor = new HashProcessor("_tag", Arrays.asList("one", "two"),
            "target", "_salt".getBytes(StandardCharsets.UTF_8), Method.SHA1, mac, true);
        IngestDocument ingestDocument = new IngestDocument(fields, new HashMap<>());
        processor.execute(ingestDocument);
        Map<String, String> target = ingestDocument.getFieldValue("target", Map.class);
        assertThat(target.size(), equalTo(1));
        assertNotNull(target.get("one"));

        HashProcessor failProcessor = new HashProcessor("_tag", Arrays.asList("one", "two"),
            "target", "_salt".getBytes(StandardCharsets.UTF_8), Method.SHA1, mac, false);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> failProcessor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [two] not present as part of path [two]"));
    }

    public void testStaticKeyAndSalt() throws Exception {
        byte[] salt = "_salt".getBytes(StandardCharsets.UTF_8);
        SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
        PBEKeySpec keySpec = new PBEKeySpec("hmackey".toCharArray(), salt, 5, 128);
        byte[] pbkdf2 = secretKeyFactory.generateSecret(keySpec).getEncoded();
        Mac mac = Mac.getInstance(Method.SHA1.getAlgorithm());
        mac.init(new SecretKeySpec(pbkdf2, Method.SHA1.getAlgorithm()));
        Map<String, Object> fields = new HashMap<>();
        fields.put("field", "0123456789");
        HashProcessor processor = new HashProcessor("_tag", Collections.singletonList("field"),
            "target", salt, Method.SHA1, mac, false);
        IngestDocument ingestDocument = new IngestDocument(fields, new HashMap<>());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target", String.class), equalTo("X3NhbHQMW0oHJGEEE9obGcGv5tGd7HFyDw=="));
    }

    public void testProcessorSingleField() throws Exception {
        List<String> fields = Collections.singletonList(randomAlphaOfLength(6));
        Map<String, Object> docFields = new HashMap<>();
        for (String field : fields) {
            docFields.put(field, randomAlphaOfLengthBetween(2, 10));
        }

        String targetField = randomAlphaOfLength(6);
        Method method = randomFrom(Method.values());
        Mac mac = createMac(method);
        byte[] salt = randomByteArrayOfLength(5);
        HashProcessor processor = new HashProcessor("_tag", fields, targetField, salt, method, mac, false);
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
        byte[] salt = randomByteArrayOfLength(5);
        HashProcessor processor = new HashProcessor("_tag", fields, targetField, salt, method, mac, false);
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
