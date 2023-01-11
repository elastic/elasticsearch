/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Encapsulates links to pages in the reference docs, so that for example we can include URLs in logs and API outputs. Each instance's
 * {@link #toString()} yields (a string representation of) a URL for the relevant docs. Links are defined in the resource file
 * {@code reference-docs-links.json} which must define links for exactly the set of values of this enum.
 */
public enum ReferenceDocs {
    INITIAL_MASTER_NODES,
    DISCOVERY_TROUBLESHOOTING,
    UNSTABLE_CLUSTER_TROUBLESHOOTING;

    private static final Map<String, String> linksBySymbol;

    static {
        try (
            var resourceStream = readFromJarResourceUrl(ReferenceDocs.class.getResource("reference-docs-links.json"));
            var parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, resourceStream)
        ) {
            linksBySymbol = Map.copyOf(parser.map(HashMap::new, XContentParser::text));
        } catch (IOException e) {
            assert false : e;
            throw new IllegalStateException("could not read links resource", e);
        }

        var symbols = Arrays.stream(ReferenceDocs.values()).map(Enum::name).collect(Collectors.toSet());
        if (symbols.equals(linksBySymbol.keySet()) == false) {
            assert false : symbols + " vs " + linksBySymbol;
            throw new IllegalStateException("symbols do not match links: " + symbols + " vs " + linksBySymbol);
        }
    }

    static final String UNRELEASED_VERSION_COMPONENT = "master";
    static final String VERSION_COMPONENT = getVersionComponent(Version.CURRENT, Build.CURRENT.isSnapshot());

    /**
     * Compute the version component of the URL path (e.g. {@code 8.5} or {@code master}) for a particular version of Elasticsearch. Exposed
     * for testing, but all items use {@link #VERSION_COMPONENT} ({@code getVersionComponent(Version.CURRENT, Build.CURRENT.isSnapshot())})
     * which relates to the current version and build.
     */
    static String getVersionComponent(Version version, boolean isSnapshot) {
        return isSnapshot && version.revision == 0 ? UNRELEASED_VERSION_COMPONENT : version.major + "." + version.minor;
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
