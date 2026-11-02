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

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

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

    /**
     * The other identity this configuration derives, and the one the listing cache carries. A listing is what a
     * principal can <i>see</i>, so it must be isolated by credential — the opposite of the schema identity above,
     * which must not be.
     * <p>
     * Derived through {@code fromQueryConfig}, so this is the value production computes, and asserted over
     * {@link S3Configuration#secretFieldNames()} rather than a name written here: every field the configuration
     * declares secret must move it. The defect this closes was a list of seven credential names inside the cache
     * that omitted {@code session_token}, so two roles over one bucket addressed one listing — and on the inline
     * path, where there is no definition version, nothing else separated them.
     */
    public void testEveryDeclaredSecretMovesTheCredentialIdentity() {
        Set<String> secrets = S3Configuration.secretFieldNames();
        assertFalse("the configuration must declare some secrets, or this asserts nothing", secrets.isEmpty());
        int checked = 0;
        for (Map<String, Object> base : List.of(EXPLICIT, FEDERATED)) {
            String identity = S3Configuration.fromQueryConfig(base).secretIdentity();
            // Federated authentication declares no secret — a role arn and an audience are not credentials — so an
            // empty identity there is the right answer, and asserting otherwise would assert the wrong contract.
            boolean carriesASecret = S3Configuration.fromQueryConfig(base).consumedKeys().stream().anyMatch(secrets::contains);
            assertEquals(
                "a credential identity exists exactly when a declared secret is carried",
                carriesASecret,
                identity.isEmpty() == false
            );
            for (String secret : S3Configuration.fromQueryConfig(base).consumedKeys()) {
                if (secrets.contains(secret) == false) {
                    continue;
                }
                assertNotEquals(
                    "secret [" + secret + "] must move the credential identity: a listing is isolated by credential",
                    identity,
                    S3Configuration.fromQueryConfig(with(base, secret, "rotated-" + secret)).secretIdentity()
                );
                assertFalse(
                    "a secret's value must not survive into the credential identity",
                    identity.contains(String.valueOf(base.get(secret)))
                );
                checked++;
            }
        }
        assertThat("both bases together must have exercised several declared secrets", checked, greaterThanOrEqualTo(3));
    }

    /** A store reached anonymously has no credential to isolate by, and an empty identity is the right answer. */
    public void testAnAnonymousStoreHasNoCredentialIdentity() {
        assertEquals("", S3Configuration.fromQueryConfig(Map.of("endpoint", "https://s3.example", "auth", "anonymous")).secretIdentity());
    }

}
