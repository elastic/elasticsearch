/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.action.EnrichCoordinatorProxyAction;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ENRICH_ORIGIN;

final class EnrichProcessorFactory implements Processor.Factory, Consumer<ClusterState> {

    static final String TYPE = "enrich";
    private final Client client;
    private final ScriptService scriptService;
    private final EnrichCache enrichCache;

    volatile Metadata metadata;

    EnrichProcessorFactory(Client client, ScriptService scriptService, EnrichCache enrichCache) {
        this.client = client;
        this.scriptService = scriptService;
        this.enrichCache = Objects.requireNonNull(enrichCache);
    }

    @Override
    public Processor create(
        Map<String, Processor.Factory> processorFactories,
        String tag,
        String description,
        Map<String, Object> config,
        ProjectId projectId
    ) throws Exception {
        final String policyName = ConfigurationUtils.readStringProperty(TYPE, tag, config, "policy_name");
        final String indexAlias = EnrichPolicy.getBaseName(policyName);
        if (metadata == null) {
            throw new IllegalStateException("enrich processor factory has not yet been initialized with cluster state");
        }
        final var project = metadata.getProject(projectId);
        IndexAbstraction indexAbstraction = project.getIndicesLookup().get(indexAlias);
        if (indexAbstraction == null) {
            throw new IllegalArgumentException("no enrich index exists for policy with name [" + policyName + "]");
        }
        assert indexAbstraction.getType() == IndexAbstraction.Type.ALIAS;
        assert indexAbstraction.getIndices().size() == 1;
        IndexMetadata imd = project.index(indexAbstraction.getIndices().get(0));

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
        var searchRunner = createSearchRunner(project, indexAlias);
        switch (policyType) {
            case EnrichPolicy.MATCH_TYPE:
            case EnrichPolicy.RANGE_TYPE:
                return new MatchProcessor(
                    tag,
                    description,
                    searchRunner,
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
                Orientation orientation = Orientation.fromString(orientationStr);
                return new GeoMatchProcessor(
                    tag,
                    description,
                    searchRunner,
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

    private SearchRunner createSearchRunner(ProjectMetadata project, String indexAlias) {
        Client originClient = new OriginSettingClient(client, ENRICH_ORIGIN);
        return (value, maxMatches, reqSupplier, handler) -> {
            // intentionally non-locking for simplicity...it's OK if we re-put the same key/value in the cache during a race condition.
            enrichCache.computeIfAbsent(
                project.id(),
                getEnrichIndexKey(project, indexAlias),
                value,
                maxMatches,
                (searchResponseActionListener) -> originClient.execute(
                    EnrichCoordinatorProxyAction.INSTANCE,
                    reqSupplier.get(),
                    searchResponseActionListener
                ),
                ActionListener.wrap(resp -> handler.accept(resp, null), e -> handler.accept(null, e))
            );
        };
    }

    private String getEnrichIndexKey(ProjectMetadata project, String indexAlias) {
        IndexAbstraction ia = project.getIndicesLookup().get(indexAlias);
        if (ia == null) {
            throw new IndexNotFoundException("no generated enrich index [" + indexAlias + "]");
        }
        return ia.getIndices().get(0).getName();
    }

    public interface SearchRunner {
        void accept(
            Object value,
            int maxMatches,
            Supplier<SearchRequest> searchRequestSupplier,
            BiConsumer<List<Map<?, ?>>, Exception> handler
        );
    }
}
