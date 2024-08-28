/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.Build;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Encapsulates links to pages in the reference docs, so that for example we can include URLs in logs and API outputs. Each instance's
 * {@link #toString()} yields (a string representation of) a URL for the relevant docs. Links are defined in the resource file
 * {@code reference-docs-links.json} which must include definitions for exactly the set of values of this enum.
 */
public enum ReferenceDocs {
    /*
     * Note that the docs subsystem parses {@code reference-docs-links.json} with regexes, not a JSON parser, so the whitespace in the file
     * is important too. See {@code sub check_elasticsearch_links} in {@code https://github.com/elastic/docs/blob/master/build_docs.pl} for
     * more details.
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
    BOOTSTRAP_CHECK_G1GC,
    BOOTSTRAP_CHECK_ALL_PERMISSION,
    BOOTSTRAP_CHECK_DISCOVERY_CONFIGURATION,
    BOOTSTRAP_CHECKS,
    BOOTSTRAP_CHECK_ENCRYPT_SENSITIVE_DATA,
    BOOTSTRAP_CHECK_PKI_REALM,
    BOOTSTRAP_CHECK_ROLE_MAPPINGS,
    BOOTSTRAP_CHECK_TLS,
    BOOTSTRAP_CHECK_TOKEN_SSL,
    BOOTSTRAP_CHECK_SECURITY_MINIMAL_SETUP,
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
    // this comment keeps the ';' on the next line so every entry above has a trailing ',' which makes the diff for adding new links cleaner
    ;

    private static final Map<String, String> linksBySymbol;

    static {
        try (var resourceStream = readFromJarResourceUrl(ReferenceDocs.class.getResource("reference-docs-links.json"))) {
            linksBySymbol = Map.copyOf(readLinksBySymbol(resourceStream));
        } catch (Exception e) {
            assert false : e;
            throw new IllegalStateException("could not read links resource", e);
        }
    }

    static final String UNRELEASED_VERSION_COMPONENT = "master";
    static final String CURRENT_VERSION_COMPONENT = "current";
    static final String VERSION_COMPONENT = getVersionComponent(Build.current().version(), Build.current().isSnapshot());

    static Map<String, String> readLinksBySymbol(InputStream inputStream) throws Exception {
        try (var parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, inputStream)) {
            final var result = parser.map(LinkedHashMap::new, XContentParser::text);
            final var iterator = result.keySet().iterator();
            for (int i = 0; i < values().length; i++) {
                final var expected = values()[i].name();
                if (iterator.hasNext() == false) {
                    throw new IllegalStateException("ran out of values at index " + i + ": expecting " + expected);
                }
                final var actual = iterator.next();
                if (actual.equals(expected) == false) {
                    throw new IllegalStateException("mismatch at index " + i + ": found " + actual + " but expected " + expected);
                }
            }
            if (iterator.hasNext()) {
                throw new IllegalStateException("found unexpected extra value: " + iterator.next());
            }
            return result;
        }
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
