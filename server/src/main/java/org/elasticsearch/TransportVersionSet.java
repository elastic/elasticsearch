/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransportVersionSet {

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField VERSIONS = new ParseField("versions");

    private static final ConstructingObjectParser<TransportVersionSet, Integer> PARSER = new ConstructingObjectParser<>(
        TransportVersionSet.class.getCanonicalName(),
        false,
        (args, latestTransportId) -> {
            String name = (String) args[0];
            int[] ids = (int[]) args[1];
            List<TransportVersion> versions = new ArrayList<>(ids.length);
            for (int id = 0; id < ids.length; ++id) {
                TransportVersion version = new TransportVersion(id);
                if (id <= latestTransportId) {
                    versions.add(version);
                }
            }
            if (versions.isEmpty()) {
                // TODO: throw
            }
            return new TransportVersionSet(name, Collections.unmodifiableList(versions));
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareIntArray(ConstructingObjectParser.constructorArg(), VERSIONS);
    }

    private static final Map<String, TransportVersionSet> TRANSPORT_VERSION_SETS;

    static {
        Map<String, TransportVersionSet> transportVersionSets = new HashMap<>();
        // TODO: load
        TRANSPORT_VERSION_SETS = Collections.unmodifiableMap(transportVersionSets);
    }

    public static TransportVersionSet get(String name) {
        TransportVersionSet transportVersionSet = TRANSPORT_VERSION_SETS.get(name);
        if (transportVersionSet == null) {
            // TODO: throw
        }
        return transportVersionSet;
    }


    private final String name;
    private final List<TransportVersion> versions;

    private TransportVersionSet(String name, List<TransportVersion> versions) {
        this.name = name;
        this.versions = versions;
    }

    public String getName() {
        return name;
    }

    public boolean isCompatible(TransportVersion version) {
        boolean compatible = version.onOrAfter(versions.get(0));
        for (int v = 1; v < versions.size(); ++v) {
            compatible |= version.isPatchFrom(versions.get(v));
        }
        return compatible;
    }

    public static TransportVersionSet fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    // TODO: Should this be in this class?
    private static TransportVersionSet fromJSONInputStream(InputStream inputStream) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, inputStream);
        return TransportVersionSet.fromXContent(parser);
    }


    private static class VersionsHolder {
        private static final String MANIFEST_LOCATION = "META-INF/transport-versions-files-manifest.txt";
        private static final String METADATA_LOCATION = "org/elasticsearch/transport/";
        private static final String LATEST_SUFFIX = "-LATEST";


        private static final List<TransportVersion> ALL_VERSIONS_SORTED;
        private static final Map<Integer, TransportVersion> ALL_VERSIONS_MAP;
        private static final Map<String, TransportVersionSet> ALL_VERSIONS_SETS_MAP;
        private static final TransportVersionSet LATEST;

        static {
            var locallyDefinedVersionSets = loadTransportVersionSets();
            var extendedVersionSets = new ArrayList<TransportVersionSet>(); // TODO add SPI for serverless

            var allVersionSets = Stream.concat(
                locallyDefinedVersionSets.stream(),
                extendedVersionSets.stream()
            ).toList();

            ALL_VERSIONS_SORTED = allVersionSets.stream().flatMap(tvSet -> tvSet.versions.stream()).sorted().toList();

            ALL_VERSIONS_MAP = ALL_VERSIONS_SORTED.stream()
                .collect(Collectors.toUnmodifiableMap(TransportVersion::id, Function.identity()));

            ALL_VERSIONS_SETS_MAP = allVersionSets.stream()
                .collect(Collectors.toUnmodifiableMap(TransportVersionSet::getName, Function.identity()));

            LATEST = getLatestTVSet();
        }

        private static TransportVersionSet getLatestTVSet() {

            var major = Version.CURRENT.major;
            var minor = Version.CURRENT.minor;
            var fileName =  major + "." + minor + "-" + LATEST_SUFFIX;

            var path = METADATA_LOCATION + fileName;
            return loadTransportVersionSet(path); // todo, use a different format?
        }

        private static List<TransportVersionSet> loadTransportVersionSets() {
            var transportVersionSets = new ArrayList<TransportVersionSet>();
            for (var fileName : loadTransportVersionSetFileNames()) {
                var path = METADATA_LOCATION + fileName;
                transportVersionSets.add(loadTransportVersionSet(path));
            }
            return transportVersionSets;
        }

        // TODO will getResourceAsStream work for IntelliJ?
        private static List<String> loadTransportVersionSetFileNames() {
            try (var manifestIS = VersionsHolder.class.getResourceAsStream(MANIFEST_LOCATION)) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(manifestIS));
                return reader.lines().filter(line -> line.isBlank() == false).toList();
            } catch (IOException e) {
                throw new RuntimeException("Transport version metadata manifest file not found at " + MANIFEST_LOCATION, e);
            }
        }

        private static TransportVersionSet loadTransportVersionSet(String path) {
            try (var fileStream = VersionsHolder.class.getResourceAsStream(path)) {
                if (fileStream != null) {
                    return TransportVersionSet.fromJSONInputStream(fileStream);
                } else {
                    throw new RuntimeException("Input stream is null");
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to load TransportVersionSet at path: " + path +
                    " specified in the manifest file: " + MANIFEST_LOCATION, e);
            }
        }
    }

}
