/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;

public final class TransformIndex {
    private static final Logger logger = LogManager.getLogger(TransformIndex.class);

    private static final String PROPERTIES = "properties";
    private static final String TYPE = "type";
    private static final String META = "_meta";

    private TransformIndex() {
    }

    public static void createDestinationIndex(Client client,
                                              Clock clock,
                                              TransformConfig transformConfig,
                                              Map<String, String> mappings,
                                              ActionListener<Boolean> listener) {
        CreateIndexRequest request = new CreateIndexRequest(transformConfig.getDestination().getIndex());

        // TODO: revisit number of shards, number of replicas
        request.settings(Settings.builder() // <1>
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1"));

        request.mapping(createMappingXContent(mappings, transformConfig.getId(), clock));

        client.execute(CreateIndexAction.INSTANCE, request, ActionListener.wrap(createIndexResponse -> {
            listener.onResponse(true);
        }, e -> {
            String message = TransformMessages.getMessage(TransformMessages.FAILED_TO_CREATE_DESTINATION_INDEX,
                    transformConfig.getDestination().getIndex(), transformConfig.getId());
            logger.error(message);
            listener.onFailure(new RuntimeException(message, e));
        }));
    }

    private static XContentBuilder createMappingXContent(Map<String, String> mappings,
                                                         String id,
                                                         Clock clock) {
        try {
            XContentBuilder builder = jsonBuilder().startObject();
            builder.startObject(SINGLE_MAPPING_NAME);
            addProperties(builder, mappings);
            addMetaData(builder, id, clock);
            builder.endObject(); // _doc type
            return builder.endObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static XContentBuilder addProperties(XContentBuilder builder,
                                                 Map<String, String> mappings) throws IOException {
        builder.startObject(PROPERTIES);
        for (Entry<String, String> field : mappings.entrySet()) {
            String fieldName = field.getKey();
            String fieldType = field.getValue();

            builder.startObject(fieldName);
            builder.field(TYPE, fieldType);

            builder.endObject();
        }
        builder.endObject(); // PROPERTIES
        return builder;
    }

    private static XContentBuilder addMetaData(XContentBuilder builder, String id, Clock clock) throws IOException {
        return builder.startObject(META)
            .field(TransformField.CREATED_BY, TransformField.TRANSFORM_SIGNATURE)
            .startObject(TransformField.META_FIELDNAME)
                .field(TransformField.CREATION_DATE_MILLIS, clock.millis())
                .startObject(TransformField.VERSION.getPreferredName())
                    .field(TransformField.CREATED, Version.CURRENT)
                .endObject()
                .field(TransformField.TRANSFORM, id)
            .endObject() // META_FIELDNAME
        .endObject(); // META
    }
}
