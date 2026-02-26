/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Aggregated capability declarations from all registered {@link DataSourcePlugin} instances.
 * Built once at startup from cheap SPI calls; does not store plugin references.
 */
public record DataSourceCapabilities(Set<String> schemes, Set<String> formats, Set<String> extensions, Set<String> catalogs) {

    public boolean supportsScheme(String s) {
        return s != null && schemes.contains(s.toLowerCase(Locale.ROOT));
    }

    public boolean supportsFormat(String f) {
        return f != null && formats.contains(f.toLowerCase(Locale.ROOT));
    }

    public boolean supportsExtension(String ext) {
        if (ext == null) {
            return false;
        }
        String normalized = ext.toLowerCase(Locale.ROOT);
        if (normalized.startsWith(".") == false) {
            normalized = "." + normalized;
        }
        return extensions.contains(normalized);
    }

    public boolean supportsCatalog(String c) {
        return c != null && catalogs.contains(c.toLowerCase(Locale.ROOT));
    }

    public String supportedSchemesString() {
        return String.join(", ", schemes);
    }

    public static DataSourceCapabilities build(List<DataSourcePlugin> plugins) {
        Set<String> allSchemes = new LinkedHashSet<>();
        Set<String> allFormats = new LinkedHashSet<>();
        Set<String> allExtensions = new LinkedHashSet<>();
        Set<String> allCatalogs = new LinkedHashSet<>();

        for (DataSourcePlugin plugin : plugins) {
            for (String scheme : plugin.supportedSchemes()) {
                allSchemes.add(scheme.toLowerCase(Locale.ROOT));
            }
            for (String scheme : plugin.supportedConnectorSchemes()) {
                allSchemes.add(scheme.toLowerCase(Locale.ROOT));
            }
            for (String format : plugin.supportedFormats()) {
                String lower = format.toLowerCase(Locale.ROOT);
                if (allFormats.contains(lower)) {
                    throw new IllegalArgumentException("Format reader for [" + format + "] is already registered");
                }
                allFormats.add(lower);
            }
            for (String ext : plugin.supportedExtensions()) {
                String normalized = ext.toLowerCase(Locale.ROOT);
                if (normalized.startsWith(".") == false) {
                    normalized = "." + normalized;
                }
                allExtensions.add(normalized);
            }
            for (String catalog : plugin.supportedCatalogs()) {
                String lower = catalog.toLowerCase(Locale.ROOT);
                if (allCatalogs.contains(lower)) {
                    throw new IllegalArgumentException("Source factory for type [" + catalog + "] is already registered");
                }
                allCatalogs.add(lower);
            }
        }

        return new DataSourceCapabilities(
            Collections.unmodifiableSet(allSchemes),
            Collections.unmodifiableSet(allFormats),
            Collections.unmodifiableSet(allExtensions),
            Collections.unmodifiableSet(allCatalogs)
        );
    }
}
