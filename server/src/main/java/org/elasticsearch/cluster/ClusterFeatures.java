/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Stores information on what features are present throughout the cluster
 */
public class ClusterFeatures implements Diffable<ClusterFeatures>, ChunkedToXContentObject {

    /**
     * The features on each individual node
     */
    private final Map<String, Set<String>> nodeFeatures;
    /**
     * The features present on all nodes
     */
    private Set<String> allNodeFeatures;

    public ClusterFeatures(Map<String, Set<String>> nodeFeatures) {
        this.nodeFeatures = nodeFeatures.entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> Set.copyOf(e.getValue())));
    }

    public static Set<String> calculateAllNodeFeatures(Collection<Set<String>> nodeFeatures) {
        if (nodeFeatures.isEmpty()) {
            return Set.of();
        }

        Set<String> allNodeFeatures = null;
        for (Set<String> featureSet : nodeFeatures) {
            if (allNodeFeatures == null) {
                allNodeFeatures = new HashSet<>(featureSet);
            } else {
                allNodeFeatures.retainAll(featureSet);
            }
        }
        return allNodeFeatures;
    }

    /**
     * The features reported by each node in the cluster.
     * <p>
     * NOTE: This should not be used directly.
     * Please use {@link org.elasticsearch.features.FeatureService#clusterHasFeature} instead.
     */
    public Map<String, Set<String>> nodeFeatures() {
        return nodeFeatures;
    }

    private Set<String> allNodeFeatures() {
        if (allNodeFeatures == null) {
            allNodeFeatures = Set.copyOf(calculateAllNodeFeatures(nodeFeatures.values()));
        }
        return allNodeFeatures;
    }

    /**
     * Returns {@code true} if {@code node} can have assumed features.
     * @see org.elasticsearch.env.BuildVersion#canRemoveAssumedFeatures
     */
    public static boolean featuresCanBeAssumedForNode(DiscoveryNode node) {
        return node.getBuildVersion().canRemoveAssumedFeatures();
    }

    /**
     * Returns {@code true} if one or more nodes in {@code nodes} can have assumed features.
     * @see org.elasticsearch.env.BuildVersion#canRemoveAssumedFeatures
     */
    public static boolean featuresCanBeAssumedForNodes(DiscoveryNodes nodes) {
        return nodes.getAllNodes().stream().anyMatch(n -> n.getBuildVersion().canRemoveAssumedFeatures());
    }

    /**
     * {@code true} if {@code feature} is present on all nodes in the cluster.
     * <p>
     * NOTE: This should not be used directly.
     * Please use {@link org.elasticsearch.features.FeatureService#clusterHasFeature} instead.
     */
    public boolean clusterHasFeature(DiscoveryNodes nodes, NodeFeature feature) {
        assert nodes.getNodes().keySet().equals(nodeFeatures.keySet())
            : "Cluster features nodes " + nodeFeatures.keySet() + " is different to discovery nodes " + nodes.getNodes().keySet();

        // basic case
        boolean allNodesHaveFeature = allNodeFeatures().contains(feature.id());
        if (allNodesHaveFeature) {
            return true;
        }

        // if the feature is assumed, check the versions more closely
        // it's actually ok if the feature is assumed, and all nodes missing the feature can assume it
        // TODO: do we need some kind of transient cache of this calculation?
        if (feature.assumedAfterNextCompatibilityBoundary()) {
            for (var nf : nodeFeatures.entrySet()) {
                if (nf.getValue().contains(feature.id()) == false
                    && featuresCanBeAssumedForNode(nodes.getNodes().get(nf.getKey())) == false) {
                    return false;
                }
            }

            // all nodes missing the feature can assume it - so that's alright then
            return true;
        }

        return false;
    }

    /**
     * Writes a canonical set of feature sets to {@code StreamOutput}.
     * This aims to minimise the data serialized by assuming that most feature sets are going to be identical
     * in any one cluster state.
     */
    private static void writeCanonicalSets(StreamOutput out, Map<String, Set<String>> featureSets) throws IOException {
        List<Set<String>> canonicalFeatureSets = new ArrayList<>();
        Map<String, Integer> nodeFeatureSetIndexes = new HashMap<>();

        IdentityHashMap<Set<String>, Integer> identityLookup = new IdentityHashMap<>();
        Map<Set<String>, Integer> lookup = new HashMap<>();
        for (var fse : featureSets.entrySet()) {
            // do a fast identity lookup first
            Integer idx = identityLookup.get(fse.getValue());
            if (idx != null) {
                nodeFeatureSetIndexes.put(fse.getKey(), idx);
                continue;
            }

            // do a contents equality lookup next
            idx = lookup.get(fse.getValue());
            if (idx != null) {
                nodeFeatureSetIndexes.put(fse.getKey(), idx);
                continue;
            }

            // we've found a new feature set - insert appropriately
            idx = canonicalFeatureSets.size();
            canonicalFeatureSets.add(fse.getValue());
            nodeFeatureSetIndexes.put(fse.getKey(), idx);
            identityLookup.put(fse.getValue(), idx);
            lookup.put(fse.getValue(), idx);
        }

        out.writeCollection(canonicalFeatureSets, (o, c) -> o.writeCollection(c, StreamOutput::writeString));
        out.writeMap(nodeFeatureSetIndexes, StreamOutput::writeVInt);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Set<String>> readCanonicalSets(StreamInput in) throws IOException {
        Set<String>[] featureSets = in.readArray(i -> i.readCollectionAsImmutableSet(StreamInput::readString), Set[]::new);
        return in.readImmutableMap(streamInput -> featureSets[streamInput.readVInt()]);
    }

    public static ClusterFeatures readFrom(StreamInput in) throws IOException {
        return new ClusterFeatures(readCanonicalSets(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeCanonicalSets(out, nodeFeatures);
    }

    @Override
    public Diff<ClusterFeatures> diff(ClusterFeatures previousState) {
        Set<String> deletes = new HashSet<>();
        Map<String, Set<String>> removals = new HashMap<>();
        Map<String, Set<String>> additions = new HashMap<>();

        for (var prevNodeFeatures : previousState.nodeFeatures.entrySet()) {
            Set<String> newFeatures = nodeFeatures.get(prevNodeFeatures.getKey());
            if (newFeatures == null) {
                deletes.add(prevNodeFeatures.getKey());
            } else {
                Set<String> removed = new HashSet<>(prevNodeFeatures.getValue());
                removed.removeAll(newFeatures);
                if (removed.isEmpty() == false) {
                    removals.put(prevNodeFeatures.getKey(), removed);
                }

                Set<String> added = new HashSet<>(newFeatures);
                added.removeAll(prevNodeFeatures.getValue());
                if (added.isEmpty() == false) {
                    additions.put(prevNodeFeatures.getKey(), added);
                }
            }
        }

        // find any completely new nodes
        for (var newNodeFeatures : nodeFeatures.entrySet()) {
            if (previousState.nodeFeatures.containsKey(newNodeFeatures.getKey()) == false) {
                additions.put(newNodeFeatures.getKey(), newNodeFeatures.getValue());
            }
        }

        return new ClusterFeaturesDiff(deletes, removals, additions);
    }

    public static Diff<ClusterFeatures> readDiffFrom(StreamInput in) throws IOException {
        return new ClusterFeaturesDiff(in);
    }

    private static class ClusterFeaturesDiff implements Diff<ClusterFeatures> {

        private final Set<String> deletes;
        private final Map<String, Set<String>> removals;
        private final Map<String, Set<String>> additions;

        private ClusterFeaturesDiff(Set<String> deletes, Map<String, Set<String>> removals, Map<String, Set<String>> additions) {
            this.deletes = deletes;
            this.removals = removals;
            this.additions = additions;
        }

        private ClusterFeaturesDiff(StreamInput in) throws IOException {
            deletes = in.readCollectionAsImmutableSet(StreamInput::readString);
            removals = readCanonicalSets(in);
            additions = readCanonicalSets(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(deletes, StreamOutput::writeString);
            writeCanonicalSets(out, removals);
            writeCanonicalSets(out, additions);
        }

        @Override
        public ClusterFeatures apply(ClusterFeatures part) {
            if (deletes.isEmpty() && removals.isEmpty() && additions.isEmpty()) {
                return part;    // nothing changing
            }

            Map<String, Set<String>> newFeatures = new HashMap<>(part.nodeFeatures);
            deletes.forEach(newFeatures::remove);

            // make sure each value is mutable when we modify it
            for (var removes : removals.entrySet()) {
                newFeatures.compute(removes.getKey(), (k, v) -> v instanceof HashSet ? v : new HashSet<>(v)).removeAll(removes.getValue());
            }
            for (var adds : additions.entrySet()) {
                newFeatures.compute(adds.getKey(), (k, v) -> v == null ? new HashSet<>() : v instanceof HashSet ? v : new HashSet<>(v))
                    .addAll(adds.getValue());
            }

            return new ClusterFeatures(newFeatures);
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startArray(),
            nodeFeatures.entrySet().stream().sorted(Map.Entry.comparingByKey()).<ToXContent>map(e -> (builder, p) -> {
                String[] features = e.getValue().toArray(String[]::new);
                Arrays.sort(features);
                return builder.startObject().field("node_id", e.getKey()).array("features", features).endObject();
            }).iterator(),
            ChunkedToXContentHelper.endArray()
        );
    }

    @Override
    public String toString() {
        // sort for ease of debugging
        var features = new TreeMap<>(nodeFeatures);
        features.replaceAll((k, v) -> new TreeSet<>(v));
        return "ClusterFeatures" + features;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ClusterFeatures == false) return false;
        if (this == obj) return true;

        ClusterFeatures that = (ClusterFeatures) obj;
        return nodeFeatures.equals(that.nodeFeatures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeFeatures);
    }
}
