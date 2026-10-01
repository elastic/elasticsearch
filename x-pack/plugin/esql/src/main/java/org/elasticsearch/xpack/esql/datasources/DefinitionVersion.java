/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * A version of the stored definitions a query reads a dataset under: the dataset's own definition and
 * that of the data source it references, folded into one opaque value.
 * <p>
 * <b>Which bytes, not how they are read.</b> This covers what a query can reach — the resource, the
 * storage settings, the credentials — and deliberately not the declared mapping. A mapping decides how
 * bytes become rows, which is what {@link org.elasticsearch.xpack.esql.datasources.cache.ReadConfigFingerprint}
 * addresses; folding it here would encode the procedure that reached a read rather than the read itself,
 * so a dataset declaring exactly what inference already produced would stop sharing the entries of its
 * undeclared twin and pay a cold scan for declaring nothing.
 * <p>
 * Everything cached about a file is derived from those definitions, so every entry is <em>addressed</em> by
 * this. An edit to either — a setting, the resource pattern, an endpoint, a credential — yields a different
 * version, so entries derived under the old one are no longer reachable and age out. That replaces an
 * invalidation path from the registry to the caches: nothing has to notice a change and tell anyone about it,
 * because the address moved.
 * <p>
 * <b>Addressing is not the same as the write path honouring it.</b> Statistics enrichment finds the entries to
 * fill by sweeping the schema cache with {@code ExternalSourceCacheService.matchesContribution}, which compares
 * a path, an mtime and a format-config fingerprint and looks at neither this version nor the storage identity.
 * Two entries that this version separates are therefore still enriched by one another's harvest when they share
 * those three, which two stores serving one bucket and key written in the same second do. That is older than
 * this class and is not closed by it; a reader must not take a version in the key to mean a harvest cannot
 * cross it.
 * <p>
 * Deliberately coarse. An edit that could not have changed what is cached still changes the version,
 * and the cost is one cold read. Deciding per field which edits matter would have to be re-decided
 * for every setting added, and being wrong that way is silent — an entry keeps being served after the
 * thing it was derived from has changed.
 * <p>
 * <b>Content, not names.</b> Neither the dataset's name nor its data source's is folded in: a name
 * decides nothing about what is read, so two definitions that are equal in content address one set of
 * entries and share the work of filling them. Renaming a dataset keeps its cache warm.
 * <p>
 * <b>An encrypted secret's value is not folded in.</b> Encryption draws a fresh IV per write, so a stored
 * secret's ciphertext is not a function of the secret: folding it versioned definitions that had not changed,
 * and two data sources registered with the same settings addressed nothing in common. What is folded for such
 * a secret is its name and the key id it is stored under, which moves when the project's encryption key does.
 * Credential isolation is carried by the party holding the decrypted value instead: {@code
 * Configured.secretIdentity}, digested from plaintext and therefore equal exactly when the secrets are, and
 * the {@code StorageIdentity} a provider stamps on the objects it reads.
 * <p>
 * A cluster that has opted out of state encryption stores a secret as plaintext — {@code
 * DataSourceService.applyEncryption} returns the settings unencrypted when the encryption service is
 * unavailable and {@code cluster.state.encryption.required} is false — and there the value IS folded in,
 * because for that shape it is stable. Rotating a secret moves every address derived from it on such a
 * cluster and does not on an encrypted one. The asymmetry is in the storage shape, not decided here.
 */
public final class DefinitionVersion {

    /**
     * Key under which the version travels in a query's merged config map, alongside the settings it is
     * computed from. The underscore prefix collides with no <em>registered</em> setting name, and marks the key as
     * the framework's; it does not stop a user typing it, because the unknown-key check skips framework keys by that
     * same prefix. A map a user typed is therefore stripped of them by {@code ConfigKeyValidator.withoutFrameworkKeys}
     * before it becomes a relation's config, so only the value set here can reach a cache key.
     */
    public static final String CONFIG_KEY = "_definition_version";

    private DefinitionVersion() {}

    /**
     * The version for {@code dataset} read under {@code parent}. Both are folded in, because a dataset
     * inherits its data source's settings: rotating a credential on the source changes what every
     * dataset over it reads, and must change their versions too.
     */
    public static String of(Dataset dataset, DataSource parent) {
        StringBuilder encoded = new StringBuilder();
        append(encoded, "res", dataset.resource());
        encodeSettings(encoded, dataset.settings());
        append(encoded, "type", parent.type());
        encodeDataSourceSettings(encoded, parent);

        byte[] bytes = encoded.toString().getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());
        // Zero-padded, matching ReadConfigFingerprint: Long.toHexString does not pad, so (0x1, 0x23) and
        // (0x12, 0x3) would both render "123" — and two definitions rendering to one version share every
        // cache address, which is the failure this class exists to prevent.
        return String.format(Locale.ROOT, "%016x%016x", hash.h1, hash.h2);
    }

    /** Sorted, so two equal definitions encode identically whatever order their settings were stored in. */
    private static void encodeSettings(StringBuilder encoded, Map<String, Object> settings) {
        for (Map.Entry<String, Object> e : new TreeMap<>(settings).entrySet()) {
            append(encoded, e.getKey(), e.getValue() == null ? null : e.getValue().toString());
        }
    }

    /**
     * A data source's settings. An encrypted secret contributes its name and the key id it is stored under, never
     * its value; a secret held as plaintext contributes its value. See the class javadoc for why they differ.
     */
    private static void encodeDataSourceSettings(StringBuilder encoded, DataSource parent) {
        Map<String, String> sorted = new TreeMap<>();
        for (Map.Entry<String, DataSourceSetting> e : parent.settings()) {
            sorted.put(e.getKey(), renderSettingValue(e.getValue().rawValue()));
        }
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            append(encoded, e.getKey(), e.getValue());
        }
    }

    /** The stored value as something whose text changes whenever the value does. */
    private static String renderSettingValue(Object rawValue) {
        if (rawValue == null) {
            return null;
        }
        if (rawValue instanceof EncryptedData encrypted) {
            return encrypted.keyId();
        }
        // A value can arrive as a byte[] — generic serialization round-trips one, which is why
        // DataSourceSetting.equals compares them by content. Object.toString would render an identity hash
        // here, so every deserialization would mint a new version and the dataset would never warm.
        if (rawValue instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        return rawValue.toString();
    }

    /**
     * One field of the pre-image, length-prefixed like {@code ReadConfigFingerprint} so that no
     * user-controlled value can forge a field boundary: without it the single setting
     * {@code {"a": "1\u0000b=2"}} and the pair {@code {"a":"1","b":"2"}} encode identically.
     */
    private static void append(StringBuilder out, String name, String value) {
        out.append(name.length()).append(':').append(name);
        if (value == null) {
            out.append("-1:");
        } else {
            out.append(value.length()).append(':').append(value);
        }
    }
}
