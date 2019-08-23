/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

final class EnrichProcessorFactory implements Processor.Factory, Consumer<ClusterState> {

    static final String TYPE = "enrich";
    private final Client client;

    volatile MetaData metaData;

    EnrichProcessorFactory(Client client) {
        this.client = client;
    }

    @Override
    public Processor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config) throws Exception {
        String policyName = ConfigurationUtils.readStringProperty(TYPE, tag, config, "policy_name");
        String policyAlias = EnrichPolicy.getBaseName(policyName);
        AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(policyAlias);
        if (aliasOrIndex == null) {
            throw new IllegalArgumentException("no enrich index exists for policy with name [" + policyName + "]");
        }
        assert aliasOrIndex.isAlias();
        assert aliasOrIndex.getIndices().size() == 1;
        IndexMetaData imd = aliasOrIndex.getIndices().get(0);

        String field = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
        Map<String, Object> mappingAsMap = imd.mapping().sourceAsMap();
        String policyType =
            (String) XContentMapValues.extractValue("_meta." + EnrichPolicyRunner.ENRICH_POLICY_TYPE_FIELD_NAME, mappingAsMap);
        String matchField = (String) XContentMapValues.extractValue("_meta." + EnrichPolicyRunner.ENRICH_MATCH_FIELD_NAME, mappingAsMap);

        boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
        boolean overrideEnabled = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "override", true);
        String targetField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "target_field");;

        switch (policyType) {
            case EnrichPolicy.EXACT_MATCH_TYPE:
                return new ExactMatchProcessor(tag, client, policyName, field, targetField, matchField,
                    ignoreMissing, overrideEnabled);
            default:
                throw new IllegalArgumentException("unsupported policy type [" + policyType + "]");
        }
    }

    @Override
    public void accept(ClusterState state) {
        metaData = state.getMetaData();
    }

}
