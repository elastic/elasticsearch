/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import org.elasticsearch.Version;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service responsible for registering features this node should publish that it has.
 */
public class FeatureService {

    private static final Map<Version, Set<String>> HISTORICAL_FEATURES = Map.ofEntries(

    );

    private final Map<String, NodeFeature> features = new ConcurrentHashMap<>();
    private volatile boolean locked;

    private record Feature(String id, int era) implements NodeFeature {}

    public NodeFeature registerFeature(String id, int era) {
        if (locked) throw new IllegalStateException("The node's feature set has already been read");

        NodeFeature feature = new Feature(id, era);

        if (features.putIfAbsent(id, feature) != null) {
            throw new IllegalArgumentException("Feature " + id + " is already registered");
        }

        return feature;
    }

    public Set<NodeFeature> readRegisteredFeatures() {
        locked = true;  // we don't need proper sync here, this is just a sanity check
        return Set.copyOf(features.values());
    }
}
