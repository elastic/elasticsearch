/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

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
 *   <li>{@code format} config key — explicit format override</li>
 *   <li>File extension extracted from the source path</li>
 * </ol>
 */
public final class FormatNameResolver {

    static final String CONFIG_FORMAT = "format";
    static final String CONFIG_READER = "reader";

    private static final Map<String, String> READER_ALIAS_TO_FORMAT = Map.of(
        EsqlPlugin.READER_PARQUET_RS, EsqlPlugin.FORMAT_PARQUET,
        EsqlPlugin.READER_JAVA, EsqlPlugin.FORMAT_PARQUET_JAVA
    );

    private FormatNameResolver() {}

    /**
     * Resolves the format name from the WITH config map and/or the source path.
     *
     * @return the format name (e.g. "parquet-java", "parquet", "orc"), or null if undetermined
     */
    public static String resolve(Map<String, Object> config, String sourcePath) {
        if (config != null) {
            Object readerOverride = config.get(CONFIG_READER);
            if (readerOverride != null) {
                String alias = readerOverride.toString().toLowerCase(Locale.ROOT);
                String formatName = READER_ALIAS_TO_FORMAT.get(alias);
                if (formatName != null) {
                    return formatName;
                }
            }
            Object formatOverride = config.get(CONFIG_FORMAT);
            if (formatOverride != null) {
                String name = formatOverride.toString().toLowerCase(Locale.ROOT);
                if (name.isEmpty() == false) {
                    return name;
                }
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
