/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.test.ESTestCase;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A storage configuration says what identifies the objects it reads, and every cache key derived from those
 * objects carries that value. Two properties decide whether the cache serves a wrong answer.
 * <p>
 * Both are asserted over the fields this configuration actually declares, never over a list written here. A
 * list in a test is the same defect as a list in the cache: the incident this mechanism exists for was a
 * hand-maintained set of seven credential names that omitted {@code session_token}, and a test naming one
 * credential passes while the others go unchecked.
 */
public class S3StorageIdentityTests extends ESTestCase {

    /**
     * Explicit credentials and federated authentication are mutually exclusive — the configuration rejects a
     * config carrying both — so each property is asserted over both valid shapes rather than one impossible one.
     */
    private static final Map<String, Object> EXPLICIT = Map.of(
        "endpoint",
        "https://s3.example",
        "region",
        "us-east-1",
        "addressing_style",
        "path",
        "access_key",
        "AKIAEXAMPLE",
        "secret_key",
        "shhh",
        "session_token",
        "tok"
    );

    private static final Map<String, Object> FEDERATED = Map.of(
        "endpoint",
        "https://s3.example",
        "region",
        "us-east-1",
        "role_arn",
        "arn:aws:iam::1:role/reader",
        "role_session_name",
        "session-a",
        "jwt_audience",
        "aud",
        "sts_endpoint",
        "https://sts.example",
        "sts_region",
        "us-east-1"
    );

    private static String identityOf(Map<String, Object> config) {
        return S3Configuration.fromQueryConfig(config).identity();
    }

    /**
     * A second valid value for a field. Most fields are free text; a field with a closed vocabulary needs one of
     * its own members, and a new such field fails here with its own name rather than silently going unvaried.
     */
    private static Object alternativeFor(String field) {
        return switch (field) {
            case "addressing_style" -> "virtual_hosted";
            case "endpoint", "sts_endpoint" -> "https://other.example";
            default -> "other-" + field;
        };
    }

    private static Map<String, Object> with(Map<String, Object> base, String key, Object value) {
        Map<String, Object> copy = new HashMap<>(base);
        copy.put(key, value);
        return copy;
    }

    /**
     * No secret may reach the identity, because the identity reaches a cache key and entries are shared across
     * users. Isolation between two sets of credentials comes from the definition version instead, which folds
     * every stored setting and leaves only a digest.
     */
    public void testNoDeclaredSecretReachesTheIdentity() {
        Set<String> secrets = S3Configuration.secretFieldNames();
        assertFalse("the configuration must declare some secrets, or this asserts nothing", secrets.isEmpty());
        for (Map<String, Object> base : List.of(EXPLICIT, FEDERATED)) {
            String identity = identityOf(base);
            for (String secret : S3Configuration.fromQueryConfig(base).consumedKeys()) {
                if (secrets.contains(secret) == false) {
                    continue;
                }
                assertEquals(
                    "secret [" + secret + "] must not move the storage identity: it would put a credential in a cache key",
                    identity,
                    identityOf(with(base, secret, "rotated-" + secret))
                );
                assertFalse("a secret's value must not survive into the identity", identity.contains(String.valueOf(base.get(secret))));
            }
        }
    }

    /**
     * Every field that is not a secret does move it. Two configurations addressing different stores — a
     * different endpoint, region, or assumed role — must not share a cached listing, schema or file metadata.
     * The field set comes from what the configuration reports it consumed, so a field added later is covered
     * without this test being edited.
     */
    public void testEveryNonSecretFieldMovesTheIdentity() {
        Set<String> secrets = S3Configuration.secretFieldNames();
        for (Map<String, Object> base : List.of(EXPLICIT, FEDERATED)) {
            Set<String> consumed = S3Configuration.fromQueryConfig(base).consumedKeys();
            assertFalse("the configuration must consume the fields under test", consumed.isEmpty());
            String identity = identityOf(base);
            for (String field : consumed) {
                if (secrets.contains(field)) {
                    continue;
                }
                assertNotEquals(
                    "field [" + field + "] must move the storage identity, or two stores share one cache entry",
                    identity,
                    identityOf(with(base, field, alternativeFor(field)))
                );
            }
        }
    }

    /** An unconfigured query addresses objects by nothing, which is what the endpoint and region components it replaced held. */
    public void testAnUnconfiguredQueryHasNoIdentity() {
        assertEquals("", identityOf(Map.of()));
    }

    /** Order is not part of a configuration, so it must not be part of its identity. */
    public void testSettingOrderDoesNotMoveTheIdentity() {
        Map<String, Object> reordered = new LinkedHashMap<>();
        EXPLICIT.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey(Comparator.reverseOrder()))
            .forEach(e -> reordered.put(e.getKey(), e.getValue()));
        assertEquals(identityOf(EXPLICIT), identityOf(reordered));
    }

}
