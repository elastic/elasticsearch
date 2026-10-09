/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.io.IOException;
import java.io.UncheckedIOException;
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
 * <b>Addressing is not the same as attribution.</b> A statistics contribution says which path, at which mtime,
 * under which format config, and never which store or which definition — so a harvest cannot be attributed to one
 * of two entries that agree on those three, which two stores serving one bucket and key written in the same second
 * do. {@code ExternalSourceCacheService.collectMatchingEntries} therefore enriches none of them rather than
 * guessing. A version in the key keeps entries apart; it does not let the write path tell them apart.
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
     * Both versions are a pure function of two immutable cluster-state objects, and the rewrite that mints them
     * runs once per QUERY - so without this every query after the first rebuilds a value that cannot have changed.
     * The pre-image walks every declared column, so the waste scales with the mapping: measured at 12 microseconds
     * per query over 100 declared columns and 124 over 1000.
     * <p>
     * Keyed by the IDENTITY of the two definitions, not by their contents. {@code Dataset.equals} deep-compares the
     * mapping, which is the work this exists to avoid; and identity is exactly the right question, because the
     * metadata is replaced wholesale on an edit - a new instance means a definition that may have changed, and the
     * same instance means one that provably has not.
     * <p>
     * Bounded, so a cluster with many datasets cannot pin unbounded stale cluster-state objects here. A miss costs
     * one recomputation, which is what the uncached path cost on every query.
     */
    private static final int MEMO_ENTRIES = 1024;
    private static final Cache<Memo, String> MEMO = CacheBuilder.<Memo, String>builder().setMaximumWeight(MEMO_ENTRIES).build();

    private static String memoized(Dataset dataset, DataSource parent, boolean datasetTier) {
        Memo key = new Memo(dataset, parent, datasetTier);
        String cached = MEMO.get(key);
        if (cached != null) {
            return cached;
        }
        String computed = datasetTier ? computeOfDataset(dataset, parent) : computeOf(dataset, parent);
        MEMO.put(key, computed);
        return computed;
    }

    /**
     * An identity key over the two definitions. {@code equals} compares by reference deliberately: see
     * {@link #MEMO}. Two distinct instances that happen to be equal simply miss and recompute.
     */
    private record Memo(Dataset dataset, DataSource parent, boolean datasetTier) {
        @Override
        public boolean equals(Object o) {
            return o instanceof Memo other && dataset == other.dataset && parent == other.parent && datasetTier == other.datasetTier;
        }

        @Override
        public int hashCode() {
            return 31 * (31 * System.identityHashCode(dataset) + System.identityHashCode(parent)) + Boolean.hashCode(datasetTier);
        }
    }

    /**
     * The version for {@code dataset} read under {@code parent}. Both are folded in, because a dataset
     * inherits its data source's settings: rotating a credential on the source changes what every
     * dataset over it reads, and must change their versions too.
     * <p>
     * Murmur3-128, following {@code ReadConfigFingerprint}, and like it this guards accidental collision rather
     * than an adversary. The pre-image is written by whoever may register a dataset or a data source, which is a
     * privileged operation; a reader who could choose it could also read what it addresses.
     */
    public static String of(Dataset dataset, DataSource parent) {
        return memoized(dataset, parent, false);
    }

    private static String computeOf(Dataset dataset, DataSource parent) {
        return render(canonical(dataset, parent, false));
    }

    /** Key under which the dataset-tier version travels in a query's merged config map. See {@link #ofDataset}. */
    public static final String DATASET_CONFIG_KEY = "_dataset_version";

    /**
     * The version of one dataset <em>as a dataset</em>: everything {@link #of} folds, plus the two names and the
     * declared mapping. A dataset-level fact is determined by one definition entire, where a per-file fact is
     * reusable by any dataset reading that file. Neither metadata type carries a version counter, so the content
     * IS the version and a field left out is an edit that silently reuses the previous measurements;
     * {@code description} is left out on purpose, changing nothing a reader does.
     * <p>
     * A secret contributes what {@link #renderSettingValue} renders - the key id, for the encrypted shape - so a
     * rotation under the same key id does NOT move this version, where the {@code secretIdentity} this replaced
     * did. Two data sources are still separated by {@code parent.name()}; one source across a rotation is not,
     * and the fold it holds is a count over a file set the fingerprint pins.
     */
    public static String ofDataset(Dataset dataset, DataSource parent) {
        return memoized(dataset, parent, true);
    }

    private static String computeOfDataset(Dataset dataset, DataSource parent) {
        return render(canonical(dataset, parent, true));
    }

    /**
     * The pre-image's fixed-width rendering, shared by both versions so the two cannot drift. Zero-padded,
     * matching {@code ReadConfigFingerprint}: {@code Long.toHexString} does not pad, so (0x1, 0x23) and
     * (0x12, 0x3) would both render "123", and two definitions rendering to one version share every cache
     * address - the failure this class exists to prevent.
     */
    private static String render(String encoded) {
        byte[] bytes = encoded.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());
        return String.format(Locale.ROOT, "%016x%016x", hash.h1, hash.h2);
    }

    /**
     * The pre-image, as a canonical JSON document. Serialized rather than hand-encoded for two reasons that are
     * both about correctness, not brevity.
     * <p>
     * JSON quoting delimits every user-controlled string by construction, so no setting key and no declared column
     * name can forge a field boundary - the failure that a hand-rolled token stream needs length prefixes, block
     * counts and a test per forgery to hold off, and that two reviews found holes in anyway.
     * <p>
     * And a declared column is written by {@link DatasetFieldMapping#toXContent}, so a field added to it is folded
     * here automatically. The hand-rolled version named {@code type}, {@code path} and {@code format} one at a
     * time, which meant a new one was silently left out - an edit that would have gone on serving the previous
     * definition's measurements.
     * <p>
     * Canonical means every map is sorted: a parsed definition's iteration order is not part of its identity, and
     * two equal definitions that hashed differently would never warm.
     */
    private static String canonical(Dataset dataset, DataSource parent, boolean datasetTier) {
        try (XContentBuilder json = JsonXContent.contentBuilder()) {
            json.startObject();
            if (datasetTier) {
                // Which definition exactly. The file tier omits both names: one file's facts are reusable by any
                // dataset that reads it, so a rename must keep those entries warm.
                json.field("dataset", dataset.name());
                json.field("source", parent.name());
            }
            json.field("resource", dataset.resource());
            json.field("type", parent.type());
            json.field("settings", new TreeMap<>(renderedSettings(dataset.settings())));
            json.field("source_settings", new TreeMap<>(renderedSourceSettings(parent)));
            if (datasetTier) {
                encodeMapping(json, dataset.mapping());
            }
            json.endObject();
            return Strings.toString(json);
        } catch (IOException e) {
            // JsonXContent writes to a byte array: there is no I/O to fail, so this cannot happen in practice.
            throw new UncheckedIOException("cannot encode the definition version pre-image", e);
        }
    }

    /** Dataset settings as text. {@code null} renders distinctly from the absent key, which JSON keeps apart. */
    private static Map<String, String> renderedSettings(Map<String, Object> settings) {
        Map<String, String> rendered = new TreeMap<>();
        for (Map.Entry<String, Object> e : settings.entrySet()) {
            rendered.put(e.getKey(), e.getValue() == null ? null : e.getValue().toString());
        }
        return rendered;
    }

    /**
     * A data source's settings as text. An encrypted secret contributes its key id, never its value; a secret held
     * as plaintext contributes its value. See the class javadoc for why they differ.
     */
    private static Map<String, String> renderedSourceSettings(DataSource parent) {
        Map<String, String> rendered = new TreeMap<>();
        for (Map.Entry<String, DataSourceSetting> e : parent.settings()) {
            rendered.put(e.getKey(), renderSettingValue(e.getValue().rawValue()));
        }
        return rendered;
    }

    /**
     * The declared mapping: the dynamic mode, then each column sorted by its logical name and written by its own
     * {@link DatasetFieldMapping#toXContent}, so a field added to a declared column is folded without an edit here.
     */
    private static void encodeMapping(XContentBuilder json, @Nullable DatasetMapping mapping) throws IOException {
        DatasetMapping.Mappings mappings = mapping == null ? null : mapping.mappings();
        if (mappings == null) {
            json.nullField("mapping");
            return;
        }
        json.startObject("mapping");
        json.field("dynamic", mappings.dynamic().name());
        json.startObject("properties");
        for (Map.Entry<String, DatasetFieldMapping> e : new TreeMap<>(mappings.properties()).entrySet()) {
            json.field(e.getKey());
            e.getValue().toXContent(json, null);
        }
        json.endObject();
        json.endObject();
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

}
