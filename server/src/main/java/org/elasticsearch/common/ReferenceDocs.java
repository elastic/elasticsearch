/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.Build;
import org.elasticsearch.core.SuppressForbidden;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Encapsulates links to pages in the reference docs, so that for example we can include URLs in logs and API outputs. Each instance's
 * {@link #toString()} yields (a string representation of) a URL for the relevant docs. Links are defined in the resource file
 * {@code reference-docs-links.txt} which must include definitions for exactly the set of values of this enum.
 */
public enum ReferenceDocs {
    /*
     * Note that the docs subsystem parses {@code reference-docs-links.txt} differently. See {@code sub check_elasticsearch_links} in
     * {@code https://github.com/elastic/docs/blob/master/build_docs.pl} for more details.
     *
     * Also note that the docs are built from the HEAD of each minor release branch, so in principle docs can move around independently of
     * the ES release process. To avoid breaking any links that have been baked into earlier patch releases, you may only add links in a
     * patch release and must not modify or remove any existing links.
     */
    INITIAL_MASTER_NODES,
    DISCOVERY_TROUBLESHOOTING,
    UNSTABLE_CLUSTER_TROUBLESHOOTING,
    LAGGING_NODE_TROUBLESHOOTING,
    SHARD_LOCK_TROUBLESHOOTING,
    NETWORK_DISCONNECT_TROUBLESHOOTING,
    CONCURRENT_REPOSITORY_WRITERS,
    ARCHIVE_INDICES,
    HTTP_TRACER,
    LOGGING,
    BOOTSTRAP_CHECK_HEAP_SIZE,
    BOOTSTRAP_CHECK_FILE_DESCRIPTOR,
    BOOTSTRAP_CHECK_MEMORY_LOCK,
    BOOTSTRAP_CHECK_MAX_NUMBER_THREADS,
    BOOTSTRAP_CHECK_MAX_FILE_SIZE,
    BOOTSTRAP_CHECK_MAX_SIZE_VIRTUAL_MEMORY,
    BOOTSTRAP_CHECK_MAXIMUM_MAP_COUNT,
    BOOTSTRAP_CHECK_CLIENT_JVM,
    BOOTSTRAP_CHECK_USE_SERIAL_COLLECTOR,
    BOOTSTRAP_CHECK_SYSTEM_CALL_FILTER,
    BOOTSTRAP_CHECK_ONERROR_AND_ONOUTOFMEMORYERROR,
    BOOTSTRAP_CHECK_EARLY_ACCESS,
    BOOTSTRAP_CHECK_ALL_PERMISSION,
    BOOTSTRAP_CHECK_DISCOVERY_CONFIGURATION,
    BOOTSTRAP_CHECKS,
    BOOTSTRAP_CHECK_ENCRYPT_SENSITIVE_DATA,
    BOOTSTRAP_CHECK_PKI_REALM,
    BOOTSTRAP_CHECK_ROLE_MAPPINGS,
    BOOTSTRAP_CHECK_TLS,
    BOOTSTRAP_CHECK_TOKEN_SSL,
    CONTACT_SUPPORT,
    UNASSIGNED_SHARDS,
    EXECUTABLE_JNA_TMPDIR,
    NETWORK_THREADING_MODEL,
    ALLOCATION_EXPLAIN_API,
    NETWORK_BINDING_AND_PUBLISHING,
    SNAPSHOT_REPOSITORY_ANALYSIS,
    S3_COMPATIBLE_REPOSITORIES,
    LUCENE_MAX_DOCS_LIMIT,
    MAX_SHARDS_PER_NODE,
    FLOOD_STAGE_WATERMARK,
    X_OPAQUE_ID,
    FORMING_SINGLE_NODE_CLUSTERS,
    CIRCUIT_BREAKER_ERRORS,
    ALLOCATION_EXPLAIN_NO_COPIES,
    ALLOCATION_EXPLAIN_MAX_RETRY,
    SECURE_SETTINGS,
    // this comment keeps the ';' on the next line so every entry above has a trailing ',' which makes the diff for adding new links cleaner
    ;

    private static final Map<String, String> linksBySymbol;

    static {
        try (var resourceStream = readFromJarResourceUrl(ReferenceDocs.class.getResource("reference-docs-links.txt"))) {
            linksBySymbol = Map.copyOf(readLinksBySymbol(resourceStream));
        } catch (Exception e) {
            assert false : e;
            throw new IllegalStateException("could not read links resource", e);
        }
    }

    static final String UNRELEASED_VERSION_COMPONENT = "master";
    static final String CURRENT_VERSION_COMPONENT = "current";
    static final String VERSION_COMPONENT = getVersionComponent(Build.current().version(), Build.current().isSnapshot());

    static final int SYMBOL_COLUMN_WIDTH = 64; // increase as needed to accommodate yet longer symbols

    static Map<String, String> readLinksBySymbol(InputStream inputStream) throws IOException {
        final var padding = " ".repeat(SYMBOL_COLUMN_WIDTH);

        record LinksBySymbolEntry(String symbol, String link) implements Map.Entry<String, String> {
            @Override
            public String getKey() {
                return symbol;
            }

            @Override
            public String getValue() {
                return link;
            }

            @Override
            public String setValue(String value) {
                assert false;
                throw new UnsupportedOperationException();
            }
        }

        final var symbolCount = values().length;
        final var linksBySymbolEntries = new LinksBySymbolEntry[symbolCount];

        try (var reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            for (int i = 0; i < symbolCount; i++) {
                final var currentLine = reader.readLine();
                final var symbol = values()[i].name();
                if (currentLine == null) {
                    throw new IllegalStateException("links resource truncated at line " + (i + 1));
                }
                if (currentLine.startsWith(symbol + " ") == false) {
                    throw new IllegalStateException(
                        "unexpected symbol at line " + (i + 1) + ": expected line starting with [" + symbol + " ]"
                    );
                }
                final var link = currentLine.substring(SYMBOL_COLUMN_WIDTH).trim();
                if (Strings.hasText(link) == false) {
                    throw new IllegalStateException("no link found for [" + symbol + "] at line " + (i + 1));
                }
                final var expectedLine = (symbol + padding).substring(0, SYMBOL_COLUMN_WIDTH) + link;
                if (currentLine.equals(expectedLine) == false) {
                    throw new IllegalStateException("unexpected content at line " + (i + 1) + ": expected [" + expectedLine + "]");
                }

                // We must only link to anchors with fixed IDs (defined by [[fragment-name]] in the docs) because auto-generated fragment
                // IDs depend on the heading text and are too easy to break inadvertently. Auto-generated fragment IDs begin with "_"
                if (link.startsWith("_") || link.contains("#_")) {
                    throw new IllegalStateException(
                        "found auto-generated fragment ID in link [" + link + "] for [" + symbol + "] at line " + (i + 1)
                    );
                }
                linksBySymbolEntries[i] = new LinksBySymbolEntry(symbol, link);
            }

            if (reader.readLine() != null) {
                throw new IllegalStateException("unexpected trailing content at line " + (symbolCount + 1));
            }
        }

        return Map.ofEntries(linksBySymbolEntries);
    }

    /**
     * Compute the version component of the URL path (e.g. {@code 8.5}, {@code master} or {@code current}) for a particular version of
     * Elasticsearch. Exposed for testing, but all items use {@link #VERSION_COMPONENT}
     * ({@code getVersionComponent(Build.current().version(), Build.current().isSnapshot())}) which relates to the current version and
     * build.
     */
    static String getVersionComponent(String version, boolean isSnapshot) {

        final Pattern semanticVersionPattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d)+");

        var matcher = semanticVersionPattern.matcher(version);
        if (matcher.matches()) {
            var major = matcher.group(1);
            var minor = matcher.group(2);
            var revision = matcher.group(3);

            // x.y.z versions with z>0 mean that x.y.0 is released so have a properly versioned docs link
            if (isSnapshot && "0".equals(revision)) {
                return UNRELEASED_VERSION_COMPONENT;
            }
            return major + "." + minor;
        }
        // Non-semantic version and snapshot -> point to the preliminary documentation for a future release (master)
        if (isSnapshot) {
            return UNRELEASED_VERSION_COMPONENT;
        }
        // Non-semantic, released version -> point to latest information (current release documentation, e.g.
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-plugins.html)
        return CURRENT_VERSION_COMPONENT;
    }

    @Override
    public String toString() {
        return "https://www.elastic.co/guide/en/elasticsearch/reference/" + VERSION_COMPONENT + "/" + linksBySymbol.get(name());
    }

    @SuppressForbidden(reason = "reads resource from jar")
    private static InputStream readFromJarResourceUrl(URL source) throws IOException {
        if (source == null) {
            throw new FileNotFoundException("links resource not found at [" + source + "]");
        }
        return source.openStream();
    }
}
