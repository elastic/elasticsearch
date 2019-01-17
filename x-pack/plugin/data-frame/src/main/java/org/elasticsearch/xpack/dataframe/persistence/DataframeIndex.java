/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.dataframe.transform.DataFrameTransformConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public final class DataframeIndex {
    private static final Logger logger = LogManager.getLogger(DataframeIndex.class);

    public static final String DOC_TYPE = "_doc";
    private static final String PROPERTIES = "properties";
    private static final String TYPE = "type";

    private DataframeIndex() {
    }

    public static void createDestinationIndex(Client client, DataFrameTransformConfig transformConfig, Map<String, String> mappings,
            final ActionListener<Boolean> listener) {
        CreateIndexRequest request = new CreateIndexRequest(transformConfig.getDestinationIndex());

        // TODO: revisit number of shards, number of replicas
        request.settings(Settings.builder() // <1>
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 0));

        request.mapping(DOC_TYPE, createMappingXContent(mappings));

        client.execute(CreateIndexAction.INSTANCE, request, ActionListener.wrap(createIndexResponse -> {
            listener.onResponse(true);
        }, e -> {
            String message = DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_CREATE_DESTINATION_INDEX,
                    transformConfig.getDestinationIndex(), transformConfig.getId());
            logger.error(message);
            listener.onFailure(new RuntimeException(message, e));
        }));
    }

    private static XContentBuilder createMappingXContent(Map<String, String> mappings) {
        try {
            XContentBuilder builder = jsonBuilder().startObject();
            builder.startObject(DOC_TYPE);
            builder.startObject(PROPERTIES);
            for (Entry<String, String> field : mappings.entrySet()) {
                builder.startObject(field.getKey()).field(TYPE, field.getValue()).endObject();
            }
            builder.endObject(); // properties
            builder.endObject(); // doc_type
            return builder.endObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
