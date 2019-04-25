/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

final class EnrichProcessorFactory implements Processor.Factory {

    static final String TYPE = "enrich";

    private final Function<String, EnrichPolicy> policyLookup;
    private final Function<String, Engine.Searcher> searchProvider;

    EnrichProcessorFactory(Supplier<ClusterState> clusterStateSupplier,
                           Function<String, Engine.Searcher> searchProvider) {
        this.policyLookup = policyName -> {
            ClusterState clusterState = clusterStateSupplier.get();
            return EnrichStore.getPolicy(policyName, clusterState);
        };
        this.searchProvider = searchProvider;
    }

    @Override
    public Processor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config) throws Exception {
        String policyName = ConfigurationUtils.readStringProperty(TYPE, tag, config, "policy_name");
        EnrichPolicy policy = policyLookup.apply(policyName);
        if (policy == null) {
            throw new IllegalArgumentException("policy [" + policyName + "] does not exists");
        }

        String key = ConfigurationUtils.readStringProperty(TYPE, tag, config, "key", policy.getEnrichKey());
        boolean ignoreKeyMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "key_ignore_missing", false);

        final List<EnrichSpecification> specifications;
        final List<Map<?, ?>> specificationConfig = ConfigurationUtils.readList(TYPE, tag, config, "values");
        specifications = specificationConfig.stream()
            .map(entry -> new EnrichSpecification((String) entry.get("source"), (String) entry.get("target")))
            .collect(Collectors.toList());

        switch (policy.getType()) {
            case EnrichPolicy.EXACT_MATCH_TYPE:
                return new ExactMatchProcessor(tag, policyLookup, searchProvider, policyName, key, ignoreKeyMissing, specifications);
            default:
                throw new IllegalArgumentException("unsupported policy type [" + policy.getType() + "]");
        }
    }

    static final class EnrichSpecification {

        final String sourceField;
        final String targetField;

        EnrichSpecification(String sourceField, String targetField) {
            this.sourceField = sourceField;
            this.targetField = targetField;
        }
    }

}
