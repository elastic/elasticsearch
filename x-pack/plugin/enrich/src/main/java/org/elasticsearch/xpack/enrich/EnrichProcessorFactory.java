/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.Map;
import java.util.function.Consumer;

final class EnrichProcessorFactory implements Processor.Factory, Consumer<ClusterState> {

    static final String TYPE = "enrich";
    private final Client client;
    private final ScriptService scriptService;

    volatile Metadata metadata;

    EnrichProcessorFactory(Client client, ScriptService scriptService) {
        this.client = client;
        this.scriptService = scriptService;
    }

    @Override
    public Processor create(Map<String, Processor.Factory> processorFactories, String tag, String description, Map<String, Object> config)
        throws Exception {
        String policyName = ConfigurationUtils.readStringProperty(TYPE, tag, config, "policy_name");
        String policyAlias = EnrichPolicy.getBaseName(policyName);
        if (metadata == null) {
            throw new IllegalStateException("enrich processor factory has not yet been initialized with cluster state");
        }
        IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(policyAlias);
        if (indexAbstraction == null) {
            throw new IllegalArgumentException("no enrich index exists for policy with name [" + policyName + "]");
        }
        assert indexAbstraction.getType() == IndexAbstraction.Type.ALIAS;
        assert indexAbstraction.getIndices().size() == 1;
        IndexMetadata imd = indexAbstraction.getIndices().get(0);

        Map<String, Object> mappingAsMap = imd.mapping().sourceAsMap();
        String policyType = (String) XContentMapValues.extractValue(
            "_meta." + EnrichPolicyRunner.ENRICH_POLICY_TYPE_FIELD_NAME,
            mappingAsMap
        );
        String matchField = (String) XContentMapValues.extractValue("_meta." + EnrichPolicyRunner.ENRICH_MATCH_FIELD_NAME, mappingAsMap);

        TemplateScript.Factory field = ConfigurationUtils.readTemplateProperty(TYPE, tag, config, "field", scriptService);
        boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
        boolean overrideEnabled = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "override", true);
        TemplateScript.Factory targetField = ConfigurationUtils.readTemplateProperty(TYPE, tag, config, "target_field", scriptService);
        int maxMatches = ConfigurationUtils.readIntProperty(TYPE, tag, config, "max_matches", 1);
        if (maxMatches < 1 || maxMatches > 128) {
            throw ConfigurationUtils.newConfigurationException(TYPE, tag, "max_matches", "should be between 1 and 128");
        }

        switch (policyType) {
            case EnrichPolicy.MATCH_TYPE:
                return new MatchProcessor(
                    tag,
                    description,
                    client,
                    policyName,
                    field,
                    targetField,
                    overrideEnabled,
                    ignoreMissing,
                    matchField,
                    maxMatches
                );
            case EnrichPolicy.GEO_MATCH_TYPE:
                String relationStr = ConfigurationUtils.readStringProperty(TYPE, tag, config, "shape_relation", "intersects");
                ShapeRelation shapeRelation = ShapeRelation.getRelationByName(relationStr);
                String orientationStr = ConfigurationUtils.readStringProperty(TYPE, tag, config, "orientation", "CCW");
                ShapeBuilder.Orientation orientation = ShapeBuilder.Orientation.fromString(orientationStr);
                return new GeoMatchProcessor(
                    tag,
                    description,
                    client,
                    policyName,
                    field,
                    targetField,
                    overrideEnabled,
                    ignoreMissing,
                    matchField,
                    maxMatches,
                    shapeRelation,
                    orientation
                );
            default:
                throw new IllegalArgumentException("unsupported policy type [" + policyType + "]");
        }
    }

    @Override
    public void accept(ClusterState state) {
        metadata = state.getMetadata();
    }

}
