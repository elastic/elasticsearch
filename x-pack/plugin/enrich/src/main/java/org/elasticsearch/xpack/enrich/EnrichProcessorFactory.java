/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

final class EnrichProcessorFactory implements Processor.Factory, Consumer<ClusterState> {

    static final String TYPE = "enrich";
    private final Client client;
    volatile Map<String, EnrichPolicy> policies = Map.of();

    EnrichProcessorFactory(Client client) {
        this.client = client;
    }

    @Override
    public Processor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config) throws Exception {
        String policyName = ConfigurationUtils.readStringProperty(TYPE, tag, config, "policy_name");
        EnrichPolicy policy = policies.get(policyName);
        if (policy == null) {
            throw new IllegalArgumentException("policy [" + policyName + "] does not exists");
        }

        String enrichKey = ConfigurationUtils.readStringProperty(TYPE, tag, config, "enrich_key", policy.getEnrichKey());
        boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_missing", false);

        final List<EnrichSpecification> specifications;
        final List<Map<?, ?>> specificationConfig = ConfigurationUtils.readList(TYPE, tag, config, "enrich_values");
        specifications = specificationConfig.stream()
            // TODO: Add templating support in enrich_values source and target options
            .map(entry -> new EnrichSpecification((String) entry.get("source"), (String) entry.get("target")))
            .collect(Collectors.toList());

        for (EnrichSpecification specification : specifications) {
            if (policy.getEnrichValues().contains(specification.sourceField) == false) {
                throw new IllegalArgumentException("source field [" + specification.sourceField + "] does not exist in policy [" +
                    policyName + "]");
            }
        }

        switch (policy.getType()) {
            case EnrichPolicy.EXACT_MATCH_TYPE:
                return new ExactMatchProcessor(tag, client, policyName, enrichKey, ignoreMissing, specifications);
            default:
                throw new IllegalArgumentException("unsupported policy type [" + policy.getType() + "]");
        }
    }

    @Override
    public void accept(ClusterState state) {
        final EnrichMetadata enrichMetadata = state.metaData().custom(EnrichMetadata.TYPE);
        if (enrichMetadata == null) {
            return;
        }
        if (policies.equals(enrichMetadata.getPolicies())) {
            return;
        }

        policies = enrichMetadata.getPolicies();
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
