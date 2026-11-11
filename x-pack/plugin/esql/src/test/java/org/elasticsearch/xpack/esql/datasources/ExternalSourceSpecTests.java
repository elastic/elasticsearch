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
    public void testEqualConfigsInDifferentMapsAreOneSpec() {
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

    /** The defect itself: one resource, two formats, two specs. */
    public void testDifferingFormatIsTwoSpecs() {
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
    public void testDifferingDatasetContextIsTwoSpecs() {
        Map<String, Object> asA = config(Map.of("format", "csv"));
        asA.put(ExternalSourceResolver.DATASET_CONTEXT_KEY, Map.of("dataset", "a", "datasource", "src", "type", "s3"));
        Map<String, Object> asB = config(Map.of("format", "csv"));
        asB.put(ExternalSourceResolver.DATASET_CONTEXT_KEY, Map.of("dataset", "b", "datasource", "src", "type", "s3"));

        assertThat(new ExternalSourceSpec(PATH, asA), not(equalTo(new ExternalSourceSpec(PATH, asB))));
    }

    /**
     * {@code EncryptedData} participates in equality by content, so a config differing only in a secret's payload is
     * a different spec. In production {@code _dataset_context} already separates two datasets, so this pins the
     * value type's own behaviour rather than the production path.
     * <p>
     * Nothing upstream catches that case: {@code DefinitionVersion} folds an encrypted secret's key id and not its
     * value — a fresh IV per write makes ciphertext unstable — and folds neither name, so the two definitions have
     * <em>equal</em> versions and the data node's check against its own registry finds nothing wrong. Keyed by the
     * resource alone, both relations would have listed and inferred under whichever configuration was stored last.
     */
    public void testDifferingEncryptedSecretIsTwoSpecs() {
        Map<String, Object> withSecretA = config(Map.of("access_key", "same"));
        withSecretA.put("secret_key", new EncryptedData("key-1", new byte[] { 1, 2, 3 }));
        Map<String, Object> withSecretB = config(Map.of("access_key", "same"));
        // Same key id on purpose: that is the half DefinitionVersion folds, so the versions are equal and only the
        // payload tells these apart.
        withSecretB.put("secret_key", new EncryptedData("key-1", new byte[] { 9, 9, 9 }));

        assertThat(new ExternalSourceSpec(PATH, withSecretA), not(equalTo(new ExternalSourceSpec(PATH, withSecretB))));
    }

    /** The same secret under the same key id is one spec, so an identical pair still shares its resolution. */
    public void testIdenticalEncryptedSecretIsOneSpec() {
        Map<String, Object> one = config(Map.of("access_key", "same"));
        one.put("secret_key", new EncryptedData("key-1", new byte[] { 1, 2, 3 }));
        Map<String, Object> two = config(Map.of("access_key", "same"));
        two.put("secret_key", new EncryptedData("key-1", new byte[] { 1, 2, 3 }));

        assertThat(new ExternalSourceSpec(PATH, one), equalTo(new ExternalSourceSpec(PATH, two)));
    }

    /** And the resource still separates reads on its own, with nothing in the configuration to tell them apart. */
    public void testDifferingPathIsTwoSpecs() {
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

        ExternalSourceSpec spec = new ExternalSourceSpec(PATH, withNull);

        assertThat(spec.config(), equalTo(withNull));
        assertThat(spec, equalTo(new ExternalSourceSpec(PATH, withNull)));
    }

    /** The copy is defensive: mutating the caller's map afterwards cannot move a key already in a resolution. */
    public void testConfigIsCopiedNotAliased() {
        Map<String, Object> config = config(Map.of("format", "csv"));
        ExternalSourceSpec spec = new ExternalSourceSpec(PATH, config);

        config.put("format", "tsv");

        assertThat(spec.config(), equalTo(Map.of("format", "csv")));
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

        // Not merely absent as a substring: nothing printed may be a function of the secret. A digest or a hash of
        // the configuration would pass the assertion above while still varying with it, and on a cluster that opted
        // out of state encryption that value is the plaintext.
        Map<String, Object> otherSecret = config(Map.of("format", "csv"));
        otherSecret.put(ExternalSourceResolver.DATASOURCE_CONFIG_KEY, Map.of("access_key", "id", "secret_key", "a different one"));
        assertThat(new ExternalSourceSpec(PATH, otherSecret).toString(), equalTo(printed));
    }

    /** A pre-signed https resource carries its signature in the query string, and this is rendered into a log. */
    public void testToStringRedactsAPreSignedUrl() {
        String path = "https://host/bucket/sales.csv?X-Amz-Signature=deadbeefnotarealsignature";

        String printed = new ExternalSourceSpec(path, config(Map.of("format", "csv"))).toString();

        assertThat(printed, not(containsString("X-Amz-Signature")));
        assertThat(printed, equalTo("https://host/bucket/sales.csv"));
    }

    /** What an operator can act on: two specs over one resource are told apart by name, not by an opaque number. */
    public void testToStringNamesTheDataset() {
        Map<String, Object> config = config(Map.of("format", "csv"));
        config.put(ExternalSourceResolver.DATASET_CONTEXT_KEY, Map.of("dataset", "sales_csv", "datasource", "s3_ds", "type", "s3"));

        assertThat(new ExternalSourceSpec(PATH, config).toString(), equalTo(PATH + "[sales_csv]"));
    }
}
