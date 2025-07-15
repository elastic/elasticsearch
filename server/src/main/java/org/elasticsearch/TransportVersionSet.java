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
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportVersionSet {

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField IDS = new ParseField("ids");

    private static final ConstructingObjectParser<TransportVersionSet, Integer> PARSER = new ConstructingObjectParser<>(
        TransportVersionSet.class.getCanonicalName(),
        false,
        (args, latestTransportId) -> {
            String name = (String) args[0];
            @SuppressWarnings("unchecked")
            List<Integer> ids = (List<Integer>) args[1];
            List<TransportVersion> versions = new ArrayList<>(ids.size());
            for (int id = 0; id < ids.size(); ++id) {
                if (id <= latestTransportId) {
                    versions.add(new TransportVersion(ids.get(id)));
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
        PARSER.declareIntArray(ConstructingObjectParser.constructorArg(), IDS);
    }

    private static final Map<String, TransportVersionSet> TRANSPORT_VERSION_SETS = loadTransportVersionSets();
    public static final List<TransportVersion> TRANSPORT_VERSIONS = collectTransportVersions();

    private static Map<String, TransportVersionSet> loadTransportVersionSets() {
        Map<String, TransportVersionSet> transportVersionSets = new HashMap<>();

        String latestLocation = "transport/" + Version.CURRENT.major + "." + Version.CURRENT.minor + "-LATEST.json";
        int latestId;
        try (InputStream inputStream = TransportVersionSet.class.getResourceAsStream(latestLocation)) {
            TransportVersionSet latest = fromXContent(inputStream, Integer.MAX_VALUE);
            // TODO: validation of latest tranport version set
            latestId = latest.versions.get(0).id();
        } catch (IOException ioe) {
            throw new UncheckedIOException("latest transport version file not found at [" + latestLocation + "]", ioe);
        }

        String manifestLocation = "META-INF/transport-versions-files-manifest.txt";
        List<String> versionNames;
        try (InputStream transportVersionManifest = TransportVersionSet.class.getClassLoader().getResourceAsStream(manifestLocation)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(transportVersionManifest, StandardCharsets.UTF_8));
            versionNames = reader.lines().filter(line -> line.isBlank() == false).toList();
        } catch (IOException ioe) {
            throw new UncheckedIOException("transport version metadata manifest file not found at [" + manifestLocation + "]", ioe);
        }

        for (String name : versionNames) {
            String versionLocation = "transport/" + name;
            try (InputStream inputStream = TransportVersionSet.class.getResourceAsStream(versionLocation)) {
                TransportVersionSet transportVersionSet = TransportVersionSet.fromXContent(inputStream, latestId);
                transportVersionSets.put(transportVersionSet.name, transportVersionSet);
            } catch (IOException ioe) {
                throw new UncheckedIOException("transport version set file not found at [ " + versionLocation + "]", ioe);
            }
        }

        return Collections.unmodifiableMap(transportVersionSets);
    }

    private static List<TransportVersion> collectTransportVersions() {
        List<TransportVersion> transportVersions = new ArrayList<>();
        for (TransportVersionSet transportVersionSet : TRANSPORT_VERSION_SETS.values()) {
            transportVersions.addAll(transportVersionSet.versions);
        }
        transportVersions.sort(TransportVersion::compareTo);
        return transportVersions;
    }

    private static TransportVersionSet fromXContent(InputStream inputStream, int maxTransportId) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, inputStream);
        return PARSER.parse(parser, maxTransportId);
    }

    public static TransportVersionSet get(String name) {
        TransportVersionSet transportVersionSet = TRANSPORT_VERSION_SETS.get(name);
        if (transportVersionSet == null) {
            // TODO: throw
        }
        return transportVersionSet;
    }

    public static TransportVersion local(String name) {
        return get(name).local();
    }

    public static TransportVersion oldest(String name) {
        return get(name).oldest();
    }

    public static boolean isCompatible(String name, TransportVersion version) {
        return get(name).isCompatible(version);
    }

    private final String name;
    private final List<TransportVersion> versions;

    private TransportVersionSet(String name, List<TransportVersion> versions) {
        this.name = name;
        this.versions = versions;
    }

    public String name() {
        return name;
    }

    public TransportVersion local() {
        return versions.get(0);
    }

    public TransportVersion oldest() {
        return versions.get(versions.size() - 1);
    }

    public boolean isCompatible(TransportVersion version) {
        boolean compatible = version.onOrAfter(local());
        for (int v = 1; v < versions.size(); ++v) {
            compatible |= version.isPatchFrom(versions.get(v));
        }
        return compatible;
    }

    public boolean isNotCompatible(TransportVersion version) {
        boolean compatible = version.before(local());
        for (int v = 1; v < versions.size(); ++v) {
            compatible &= version.isPatchFrom(versions.get(v)) == false;
        }
        return compatible;
    }

    @Override
    public String toString() {
        return "TransportVersionSet{" + "name='" + name + '\'' + ", versions=" + versions + '}';
    }
}
