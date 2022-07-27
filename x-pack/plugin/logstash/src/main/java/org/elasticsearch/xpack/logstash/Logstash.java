/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.logstash;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.logstash.action.DeletePipelineAction;
import org.elasticsearch.xpack.logstash.action.GetPipelineAction;
import org.elasticsearch.xpack.logstash.action.PutPipelineAction;
import org.elasticsearch.xpack.logstash.action.TransportDeletePipelineAction;
import org.elasticsearch.xpack.logstash.action.TransportGetPipelineAction;
import org.elasticsearch.xpack.logstash.action.TransportPutPipelineAction;
import org.elasticsearch.xpack.logstash.rest.RestDeletePipelineAction;
import org.elasticsearch.xpack.logstash.rest.RestGetPipelineAction;
import org.elasticsearch.xpack.logstash.rest.RestPutPipelineAction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.elasticsearch.index.engine.EngineConfig.INDEX_CODEC_SETTING;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.LOGSTASH_MANAGEMENT_ORIGIN;

/**
 * This class supplies the logstash featureset and templates
 */
public class Logstash extends Plugin implements SystemIndexPlugin {

    public static final String LOGSTASH_CONCRETE_INDEX_NAME = ".logstash";
    public static final String LOGSTASH_INDEX_NAME_PATTERN = LOGSTASH_CONCRETE_INDEX_NAME + "*";

    static final LicensedFeature.Momentary LOGSTASH_FEATURE = LicensedFeature.momentary(null, "logstash", License.OperationMode.STANDARD);

    public Logstash() {}

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(XPackUsageFeatureAction.LOGSTASH, LogstashUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.LOGSTASH, LogstashInfoTransportAction.class),
            new ActionHandler<>(PutPipelineAction.INSTANCE, TransportPutPipelineAction.class),
            new ActionHandler<>(GetPipelineAction.INSTANCE, TransportGetPipelineAction.class),
            new ActionHandler<>(DeletePipelineAction.INSTANCE, TransportDeletePipelineAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestPutPipelineAction(), new RestGetPipelineAction(), new RestDeletePipelineAction());
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            SystemIndexDescriptor.builder()
                .setIndexPattern(LOGSTASH_INDEX_NAME_PATTERN)
                .setPrimaryIndex(LOGSTASH_CONCRETE_INDEX_NAME)
                .setDescription("Contains data for Logstash Central Management")
                .setMappings(getIndexMappings())
                .setSettings(getIndexSettings())
                .setVersionMetaKey("logstash-version")
                .setOrigin(LOGSTASH_MANAGEMENT_ORIGIN)
                .build()
        );
    }

    private Settings getIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(INDEX_CODEC_SETTING.getKey(), CodecService.BEST_COMPRESSION_CODEC)
            .build();
    }

    private XContentBuilder getIndexMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();
            {
                builder.startObject();
                {
                    builder.startObject(SINGLE_MAPPING_NAME);
                    builder.field("dynamic", "strict");
                    {
                        builder.startObject("_meta");
                        builder.field("logstash-version", Version.CURRENT);
                        builder.endObject();
                    }
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("description");
                            builder.field("type", "text");
                            builder.endObject();
                        }
                        {
                            builder.startObject("last_modified");
                            builder.field("type", "date");
                            builder.endObject();
                        }
                        {
                            builder.startObject("pipeline_metadata");
                            {
                                builder.startObject("properties");
                                {
                                    builder.startObject("version");
                                    builder.field("type", "short");
                                    builder.endObject();
                                    builder.startObject("type");
                                    builder.field("type", "keyword");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        {
                            builder.startObject("pipeline");
                            builder.field("type", "text");
                            builder.endObject();
                        }
                        {
                            builder.startObject("pipeline_settings");
                            builder.field("dynamic", false);
                            builder.field("type", "object");
                            builder.endObject();
                        }
                        {
                            builder.startObject("username");
                            builder.field("type", "keyword");
                            builder.endObject();
                        }
                        {
                            builder.startObject("metadata");
                            builder.field("dynamic", false);
                            builder.field("type", "object");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build " + LOGSTASH_CONCRETE_INDEX_NAME + " index mappings", e);
        }
    }

    @Override
    public String getFeatureName() {
        return "logstash_management";
    }

    @Override
    public String getFeatureDescription() {
        return "Enables Logstash Central Management pipeline storage";
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetadata>> getIndexTemplateMetadataUpgrader() {
        return templates -> {
            // .logstash is a system index now. deleting the legacy template
            templates.remove("logstash-index-template");
            return templates;
        };
    }
}
