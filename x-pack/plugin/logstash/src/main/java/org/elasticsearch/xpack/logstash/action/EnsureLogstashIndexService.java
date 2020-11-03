/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.indices.EnsureIndexService;
import org.elasticsearch.xpack.logstash.Logstash;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.engine.EngineConfig.INDEX_CODEC_SETTING;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;

public class EnsureLogstashIndexService extends EnsureIndexService {

    public EnsureLogstashIndexService(ClusterService clusterService, Client client) {
        super(clusterService, Logstash.LOGSTASH_CONCRETE_INDEX_NAME, client);
    }

    @Override
    protected Settings indexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(INDEX_CODEC_SETTING.getKey(), CodecService.BEST_COMPRESSION_CODEC)
            .build();
    }

    @Override
    protected XContentBuilder mappings() throws IOException {
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
    }
}
