/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * {@link ExternalSourceSpec} is the key a resolution is stored and looked up under, so what counts as the same read is
 * decided here, by equality, and nothing else in the pipeline gets a say.
 */
public class ExternalSourceSpecTests extends ESTestCase {

    private static final String PATH = "s3://bucket/sales";

    private static Map<String, Object> config(Map<String, Object> entries) {
        return new HashMap<>(entries);
    }

    /**
     * A configuration is rebuilt rather than shared: {@code DatasetRewriter.datasetConfig} returns a fresh map for
     * every relation it builds, and a test assembles its keys separately again. So the keys must compare by content
     * and not by map identity, and their hash codes with them, or a lookup misses a resolution that is sitting right
     * there — and two relations reading one dataset would resolve it twice instead of sharing.
     */
    public void testEqualConfigsInDifferentMapsAreOneRead() {
        Map<String, Object> left = new HashMap<>(Map.of("format", "csv", "schema_resolution", "union_by_name"));
        // A different implementation and a different insertion order, so only content can make these equal.
        Map<String, Object> right = new LinkedHashMap<>();
        right.put("schema_resolution", "union_by_name");
        right.put("format", "csv");

        ExternalSourceSpec a = new ExternalSourceSpec(PATH, left);
        ExternalSourceSpec b = new ExternalSourceSpec(PATH, right);

        assertThat(a, equalTo(b));
        assertThat(a.hashCode(), equalTo(b.hashCode()));
    }

    /** The defect itself: one resource, two formats, two reads. */
    public void testDifferingFormatIsTwoReads() {
        assertThat(
            new ExternalSourceSpec(PATH, config(Map.of("format", "csv"))),
            not(equalTo(new ExternalSourceSpec(PATH, config(Map.of("format", "tsv")))))
        );
    }

    /**
     * Two datasets over one resource are two reads even when only their {@code _dataset_context} differs. The context
     * does not change which bytes are read, but {@code ExternalSourceResolver} annotates a classified failure with it,
     * so collapsing the two would report one dataset's failure under the other's name.
     */
    public void testDifferingDatasetContextIsTwoReads() {
        Map<String, Object> asA = config(Map.of("format", "csv"));
        asA.put(ExternalSourceResolver.DATASET_CONTEXT_KEY, Map.of("dataset", "a", "datasource", "src", "type", "s3"));
        Map<String, Object> asB = config(Map.of("format", "csv"));
        asB.put(ExternalSourceResolver.DATASET_CONTEXT_KEY, Map.of("dataset", "b", "datasource", "src", "type", "s3"));

        assertThat(new ExternalSourceSpec(PATH, asA), not(equalTo(new ExternalSourceSpec(PATH, asB))));
    }

    /**
     * The silent half of the defect this key closes. Two data sources over one resource that differ only in their
     * credentials are two reads here, because {@code EncryptedData.equals} compares the payload.
     * <p>
     * Nothing upstream catches that case: {@code DefinitionVersion} folds an encrypted secret's key id and not its
     * value — a fresh IV per write makes ciphertext unstable — and folds neither name, so the two definitions have
     * <em>equal</em> versions and the data node's rebind check finds nothing wrong. Keyed by the resource alone,
     * both relations would have resolved under whichever configuration was stored last, credentials included.
     */
    public void testDifferingEncryptedSecretIsTwoReads() {
        Map<String, Object> withSecretA = config(Map.of("access_key", "same"));
        withSecretA.put("secret_key", new EncryptedData("key-1", new byte[] { 1, 2, 3 }));
        Map<String, Object> withSecretB = config(Map.of("access_key", "same"));
        // Same key id on purpose: that is the half DefinitionVersion folds, so the versions are equal and only the
        // payload tells these apart.
        withSecretB.put("secret_key", new EncryptedData("key-1", new byte[] { 9, 9, 9 }));

        assertThat(new ExternalSourceSpec(PATH, withSecretA), not(equalTo(new ExternalSourceSpec(PATH, withSecretB))));
    }

    /** The same secret under the same key id is one read, so an identical pair still shares its resolution. */
    public void testIdenticalEncryptedSecretIsOneRead() {
        Map<String, Object> one = config(Map.of("access_key", "same"));
        one.put("secret_key", new EncryptedData("key-1", new byte[] { 1, 2, 3 }));
        Map<String, Object> two = config(Map.of("access_key", "same"));
        two.put("secret_key", new EncryptedData("key-1", new byte[] { 1, 2, 3 }));

        assertThat(new ExternalSourceSpec(PATH, one), equalTo(new ExternalSourceSpec(PATH, two)));
    }

    /** And the resource still separates reads on its own, with nothing in the configuration to tell them apart. */
    public void testDifferingPathIsTwoReads() {
        Map<String, Object> config = config(Map.of("format", "csv"));
        assertThat(new ExternalSourceSpec("s3://bucket/left", config), not(equalTo(new ExternalSourceSpec("s3://bucket/right", config))));
    }

    /**
     * A dataset setting can carry a null value — {@code DefinitionVersion.encodeSettings} encodes one — so the
     * defensive copy cannot be {@code Map.copyOf}, which refuses them.
     */
    public void testConfigHoldingANullValueIsAccepted() {
        Map<String, Object> withNull = new HashMap<>();
        withNull.put("format", "csv");
        withNull.put("region", null);

        ExternalSourceSpec read = new ExternalSourceSpec(PATH, withNull);

        assertThat(read.config(), equalTo(withNull));
        assertThat(read, equalTo(new ExternalSourceSpec(PATH, withNull)));
    }

    /** The copy is defensive: mutating the caller's map afterwards cannot move a key already in a resolution. */
    public void testConfigIsCopiedNotAliased() {
        Map<String, Object> config = config(Map.of("format", "csv"));
        ExternalSourceSpec read = new ExternalSourceSpec(PATH, config);

        config.put("format", "tsv");

        assertThat(read.config(), equalTo(Map.of("format", "csv")));
    }

    /** A null configuration is the empty one rather than an NPE at the first lookup. */
    public void testNullConfigIsEmpty() {
        assertThat(new ExternalSourceSpec(PATH, null).config(), equalTo(Map.of()));
    }

    /**
     * A collection of these is logged when resolution retries, and printed by any assertion over a resolution's key
     * set, so {@code toString} must not carry the data source's credentials.
     */
    public void testToStringHidesTheConfiguration() {
        String secret = "AKIAnotarealsecret9999";
        Map<String, Object> config = config(Map.of("format", "csv"));
        config.put(ExternalSourceResolver.DATASOURCE_CONFIG_KEY, Map.of("access_key", "id", "secret_key", secret));

        String printed = new ExternalSourceSpec(PATH, config).toString();

        assertThat(printed, not(containsString(secret)));
        assertThat(printed, containsString(PATH));
    }
}
