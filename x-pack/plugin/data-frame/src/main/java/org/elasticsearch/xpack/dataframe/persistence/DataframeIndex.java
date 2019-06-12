/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.persistence;

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
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.SingleGroupSource;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;

public final class DataframeIndex {
    private static final Logger logger = LogManager.getLogger(DataframeIndex.class);

    private static final String PROPERTIES = "properties";
    private static final String TYPE = "type";
    private static final String FORMAT = "format";
    private static final String META = "_meta";
    private static final String DEFAULT_TIME_FORMAT = "strict_date_optional_time||epoch_millis";

    private DataframeIndex() {
    }

    public static void createDestinationIndex(Client client, DataFrameTransformConfig transformConfig, Map<String, String> mappings,
            final ActionListener<Boolean> listener) {
        CreateIndexRequest request = new CreateIndexRequest(transformConfig.getDestination().getIndex());

        // TODO: revisit number of shards, number of replicas
        request.settings(Settings.builder() // <1>
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1"));

        request.mapping(SINGLE_MAPPING_NAME, createMappingXContent(mappings,
            transformConfig.getPivotConfig().getGroupConfig().getGroups(),
            transformConfig.getId()));

        client.execute(CreateIndexAction.INSTANCE, request, ActionListener.wrap(createIndexResponse -> {
            listener.onResponse(true);
        }, e -> {
            String message = DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_CREATE_DESTINATION_INDEX,
                    transformConfig.getDestination().getIndex(), transformConfig.getId());
            logger.error(message);
            listener.onFailure(new RuntimeException(message, e));
        }));
    }

    private static XContentBuilder createMappingXContent(Map<String, String> mappings,
                                                         Map<String, SingleGroupSource> groupSources,
                                                         String id) {
        try {
            XContentBuilder builder = jsonBuilder().startObject();
            builder.startObject(SINGLE_MAPPING_NAME);
            addMetaData(builder, id);
            builder.startObject(PROPERTIES);
            for (Entry<String, String> field : mappings.entrySet()) {
                String fieldName = field.getKey();
                String fieldType = field.getValue();

                builder.startObject(fieldName);
                builder.field(TYPE, fieldType);

                SingleGroupSource groupSource = groupSources.get(fieldName);
                if (groupSource instanceof DateHistogramGroupSource) {
                    String format = ((DateHistogramGroupSource) groupSource).getFormat();
                    if (format != null) {
                        builder.field(FORMAT, DEFAULT_TIME_FORMAT + "||" + format);
                    }
                }
                builder.endObject();
            }
            builder.endObject(); // properties
            builder.endObject(); // _doc type
            return builder.endObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static XContentBuilder addMetaData(XContentBuilder builder, String id) throws IOException {
        builder.startObject(META);
        builder.field(DataFrameField.CREATED_BY, DataFrameField.DATA_FRAME_SIGNATURE);
        builder.startObject(DataFrameField.META_FIELDNAME);
        builder.field(DataFrameField.CREATION_DATE_MILLIS, System.currentTimeMillis());
        builder.startObject(DataFrameField.VERSION);
        builder.field(DataFrameField.CREATED, Version.CURRENT);
        builder.endObject();
        builder.field(DataFrameField.TRANSFORM, id);
        builder.endObject(); // META_FIELDNAME
        builder.endObject(); // META
        return builder;
    }
}
