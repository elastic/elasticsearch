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
 * Everything cached about a file is derived from those definitions, so everything cached about it is
 * addressed by this. An edit to either — a setting, the resource pattern, an endpoint, a credential —
 * yields a different version, so entries derived under the old one are no longer reachable and age out.
 * That replaces an invalidation path from the registry to the caches: nothing has to notice a change
 * and tell anyone about it, because the address moved.
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
 * Credential values are folded in, never carried: they reach only {@link MurmurHash3}, and what comes
 * out is a digest. A rotation changes the version without a secret entering a cache key.
 */
public final class DefinitionVersion {

    /**
     * Key under which the version travels in a query's merged config map, alongside the settings it is
     * computed from. Chosen to collide with no setting name a user can register.
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
        // The declared mapping decides which columns are read and at what types, so two datasets over
        // one resource that differ only in their mapping must not share a version.
        append(encoded, "map", dataset.mapping() == null ? null : dataset.mapping().toString());

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
     * A data source's settings, secrets included — folded in as the value that actually distinguishes
     * one credential from another.
     * <p>
     * A secret is stored as an {@link EncryptedData} carrier, and its {@code toString} redacts the
     * ciphertext, so letting the carrier render itself would fold in only the setting's name and the
     * project-wide key id: every rotation would produce the same version and entries harvested under
     * the old credential would stay addressable. The key id and the ciphertext are therefore read off
     * the carrier explicitly.
     * <p>
     * The ciphertext carries a fresh random IV per encryption ({@code AesGcm}), so re-encrypting an
     * unchanged secret also changes the version. That direction is safe — it costs a cold read, where
     * the direction this method exists to close costs a stale answer.
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
            return encrypted.keyId() + ':' + Base64.getEncoder().encodeToString(encrypted.payload());
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
