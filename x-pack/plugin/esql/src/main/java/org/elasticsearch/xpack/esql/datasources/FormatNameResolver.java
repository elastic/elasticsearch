/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Single source of truth for resolving format names from EXTERNAL query configuration.
 * <p>
 * Both the optimizer ({@code PushFiltersToSource}) and execution ({@code FileSourceFactory})
 * need to resolve which format reader to use for a given query. This class centralises that
 * logic so the two code paths cannot diverge.
 * <p>
 * Resolution priority:
 * <ol>
 *   <li>{@code reader} config key — reader alias mapped to format name</li>
 *   <li>{@code format} config key: explicit format override (the sentinel {@link #FORMAT_AUTO}
 *       and an empty value mean "no override" and fall through to the extension)</li>
 *   <li>File extension extracted from the source path</li>
 * </ol>
 */
public final class FormatNameResolver {

    public static final String CONFIG_FORMAT = "format";
    static final String CONFIG_READER = "reader";

    /**
     * Sentinel {@code format} value meaning "no explicit override, infer from the resource
     * extension". Shared with the dataset CRUD validator so the create path and this read path
     * treat {@code auto} identically. Without this, a stored or EXTERNAL {@code format=auto} would
     * reach {@code registry.byName("auto")} and throw.
     */
    public static final String FORMAT_AUTO = "auto";

    /** Reader alias accepted in {@code WITH {"reader": "..."}} for the parquet-rs native reader. */
    public static final String READER_PARQUET_RS = "parquet-rs";
    /** Reader alias accepted in {@code WITH {"reader": "..."}} for the Java parquet reader. */
    public static final String READER_JAVA = "java";
    /** Format name registered by the Java parquet reader. */
    public static final String FORMAT_PARQUET = "parquet";
    /** Format name registered by the parquet-rs native reader. */
    public static final String FORMAT_PARQUET_RS = "parquet-rs";

    /**
     * Gates the prototype parquet-rs native reader and its public {@code reader=parquet-rs} selector.
     * Snapshot-on, release-off; override in release with {@code -Des.esql_external_parquet_rs_feature_flag_enabled=true}.
     * Lives here (and not in the parquet-rs plugin module) so this esql-core resolver can gate the alias while
     * {@code ParquetRsPlugin} — which already depends on this class — reuses the same flag for format registration.
     */
    public static final FeatureFlag ESQL_EXTERNAL_PARQUET_RS_FEATURE_FLAG = new FeatureFlag("esql_external_parquet_rs");

    private static final Map<String, String> READER_ALIAS_TO_FORMAT = parquetRsEnabled()
        ? Map.of(READER_PARQUET_RS, FORMAT_PARQUET_RS, READER_JAVA, FORMAT_PARQUET)
        : Map.of(READER_JAVA, FORMAT_PARQUET);

    /** The parquet-rs reader is live iff the external-datasources umbrella and the parquet-rs sub-flag are both on. */
    public static boolean parquetRsEnabled() {
        return DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled() && ESQL_EXTERNAL_PARQUET_RS_FEATURE_FLAG.isEnabled();
    }

    private FormatNameResolver() {}

    /**
     * Normalizes a raw {@code format} value to its canonical stored form — {@code trim()} then
     * {@code toLowerCase(Locale.ROOT)} — or {@code null} when the value is absent. This is the single
     * source of the normalization, so the create-time storage representation and the query-time
     * resolution cannot drift. Unlike {@link #parseExplicitFormat} it does <em>not</em> collapse the
     * {@link #FORMAT_AUTO} sentinel: the CRUD validator stores {@code auto} verbatim so it round-trips,
     * whereas the read path treats it as "infer from the extension".
     */
    @Nullable
    public static String normalizeFormatValue(Object raw) {
        return raw == null ? null : raw.toString().trim().toLowerCase(Locale.ROOT);
    }

    /**
     * Resolves a raw {@code format} config value into a usable format name, or returns {@code null}
     * when the value means "infer from the resource extension" (blank, or the {@link #FORMAT_AUTO}
     * sentinel). Normalizes via {@link #normalizeFormatValue}, so a value accepted here resolves
     * identically at query time.
     *
     * <p>This is the single source of truth for {@code format} sentinel handling. Both the CRUD
     * validator ({@code FileDataSourceValidator.explicitFormat}) and the query-time resolvers
     * ({@link #resolve}, {@link #resolveReader}) delegate here so new sentinels only need one edit.
     */
    @Nullable
    public static String parseExplicitFormat(Object raw) {
        String name = normalizeFormatValue(raw);
        if (name == null || name.isEmpty() || name.equals(FORMAT_AUTO)) {
            return null;
        }
        return name;
    }

    /**
     * Resolves the format name from the WITH config map and/or the source path.
     *
     * @return the format name (e.g. "parquet", "parquet-rs", "orc"), or null if undetermined
     */
    public static String resolve(Map<String, Object> config, String sourcePath) {
        if (config != null) {
            Object readerOverride = config.get(CONFIG_READER);
            if (readerOverride != null) {
                String alias = readerOverride.toString().trim().toLowerCase(Locale.ROOT);
                String formatName = READER_ALIAS_TO_FORMAT.get(alias);
                if (formatName != null) {
                    return formatName;
                }
            }
            String name = parseExplicitFormat(config.get(CONFIG_FORMAT));
            if (name != null) {
                return name;
            }
        }
        return formatFromExtension(sourcePath);
    }

    /**
     * Maps a reader alias to its format name.
     *
     * @return the format name, or null if the alias is not recognised
     */
    public static String readerAliasToFormat(String alias) {
        return READER_ALIAS_TO_FORMAT.get(alias);
    }

    public static Set<String> supportedReaderAliases() {
        return READER_ALIAS_TO_FORMAT.keySet();
    }

    /**
     * Resolves the format reader using config and source path, looking up the result in the registry.
     * <p>
     * Config-based overrides ({@code reader}, {@code format}) are resolved via {@link #resolve} and
     * looked up by name. Extension-based resolution delegates to {@link FormatReaderRegistry#byExtension}
     * which handles compound extensions (e.g. {@code .ndjson.bz}) and compression codecs.
     */
    public static FormatReader resolveReader(Map<String, Object> config, String objectName, FormatReaderRegistry registry) {
        if (config != null) {
            Object readerOverride = config.get(CONFIG_READER);
            if (readerOverride != null) {
                String alias = readerOverride.toString().trim().toLowerCase(Locale.ROOT);
                String formatName = READER_ALIAS_TO_FORMAT.get(alias);
                if (formatName == null) {
                    throw new IllegalArgumentException("Unknown reader [" + alias + "]; supported values: " + supportedReaderAliases());
                }
                return registry.byName(formatName);
            }
            String name = parseExplicitFormat(config.get(CONFIG_FORMAT));
            if (name != null) {
                return registry.byName(name);
            }
        }
        return registry.byExtension(objectName);
    }

    private static String formatFromExtension(String sourcePath) {
        if (sourcePath == null) {
            return null;
        }
        int lastDot = sourcePath.lastIndexOf('.');
        if (lastDot < 0 || lastDot >= sourcePath.length() - 1) {
            return null;
        }
        String ext = sourcePath.substring(lastDot + 1);
        int queryStart = ext.indexOf('?');
        if (queryStart >= 0) {
            ext = ext.substring(0, queryStart);
        }
        int fragmentStart = ext.indexOf('#');
        if (fragmentStart >= 0) {
            ext = ext.substring(0, fragmentStart);
        }
        return ext.isEmpty() ? null : ext.toLowerCase(Locale.ROOT);
    }
}
