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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        // TODO: TEST ONLY
        transportVersionSets.put("ml-inference-custom-service-embedding-type", new TransportVersionSet(
            "ml-inference-custom-service-embedding-type",
            List.of(new TransportVersion(9118000))
        ));
        transportVersionSets.put("esql-local-relation-with-new-blocks", new TransportVersionSet(
            "esql-local-relation-with-new-blocks",
            List.of(new TransportVersion(9117000))
        ));
        transportVersionSets.put("esql-split-on-big-values", new TransportVersionSet(
            "esql-split-on-big-values",
            List.of(
                new TransportVersion(9116000),
                new TransportVersion(9112001),
                new TransportVersion(8841063)
            )
        ));
        transportVersionSets.put("ml-inference-ibm-watsonx-completion-added", new TransportVersionSet(
            "ml-inference-ibm-watsonx-completion-added",
            List.of(new TransportVersion(9115000))
        ));
        transportVersionSets.put("esql-serialize-timeseries-field-type", new TransportVersionSet(
                "esql-serialize-timeseries-field-type",
                List.of(new TransportVersion(9114000))
        ));
        // TODO: END TEST ONLY
        TRANSPORT_VERSION_SETS = Collections.unmodifiableMap(transportVersionSets);
    }

    public static TransportVersionSet get(String name) {
        TransportVersionSet transportVersionSet = TRANSPORT_VERSION_SETS.get(name);
        if (transportVersionSet == null) {
            // TODO: throw
        }
        return transportVersionSet;
    }

    public static TransportVersion latest(String name) {
        return get(name).latest();
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

    public TransportVersion latest() {
        return versions.get(0);
    }

    public boolean isCompatible(TransportVersion version) {
        boolean compatible = version.onOrAfter(latest());
        for (int v = 1; v < versions.size(); ++v) {
            compatible |= version.isPatchFrom(versions.get(v));
        }
        return compatible;
    }

    @Override
    public String toString() {
        return "TransportVersionSet{" + "name='" + name + '\'' + ", versions=" + versions + '}';
    }
}
