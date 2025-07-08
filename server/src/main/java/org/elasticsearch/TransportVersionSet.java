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

    public boolean isCompatible(TransportVersion version) {
        boolean compatible = version.onOrAfter(versions.get(0));
        for (int v = 1; v < versions.size(); ++v) {
            compatible |= version.isPatchFrom(versions.get(v));
        }
        return compatible;
    }
}
