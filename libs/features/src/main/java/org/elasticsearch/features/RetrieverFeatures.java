package org.elasticsearch.features;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

/**
 * Shared feature definitions for retrievers.
 */
public class RetrieverFeatures implements FeatureSpecification {

    public static final NodeFeature PINNED_RETRIEVER_FEATURE = new NodeFeature("pinned_retriever_supported");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(PINNED_RETRIEVER_FEATURE);
    }
} 