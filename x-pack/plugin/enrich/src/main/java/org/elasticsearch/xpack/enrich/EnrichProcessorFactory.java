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

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

final class EnrichProcessorFactory implements Processor.Factory, Consumer<ClusterState> {

    static final String TYPE = "enrich";
    private final Client client;
    volatile Map<String, EnrichPolicy> policies = Collections.emptyMap();

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

        String field = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
        boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
        boolean overrideEnabled = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "override", true);
        String targetField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "target_field");;

        switch (policy.getType()) {
            case EnrichPolicy.EXACT_MATCH_TYPE:
                return new ExactMatchProcessor(tag, client, policyName, field, targetField, policy.getMatchField(),
                    ignoreMissing, overrideEnabled);
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

}
