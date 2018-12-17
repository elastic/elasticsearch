/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.persistence;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import java.io.IOException;
import java.util.Collections;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public final class DataFrameInternalIndex {

    // constants for the index
    public static final String INDEX_TEMPLATE_VERSION = "1";
    public static final String INDEX_TEMPLATE_PATTERN = ".data-frame-internal-";
    public static final String INDEX_TEMPLATE_NAME = INDEX_TEMPLATE_PATTERN + INDEX_TEMPLATE_VERSION;
    public static final String INDEX_NAME = INDEX_TEMPLATE_NAME;

    // constants for mappings
    public static final String ENABLED = "enabled";
    public static final String DYNAMIC = "dynamic";
    public static final String PROPERTIES = "properties";
    public static final String TYPE = "type";

    // data types
    public static final String DOUBLE = "double";
    public static final String KEYWORD = "keyword";

    // internal document types, e.g. "job_config"
    public static final String DOC_TYPE = "doc_type";

    public static void createIndexTemplate(Client client, final ActionListener<Boolean> finalListener) {
        PutIndexTemplateRequest request;
        try {
            request = new PutIndexTemplateRequest(INDEX_TEMPLATE_NAME)
                    .patterns(Collections.singletonList(INDEX_TEMPLATE_NAME))
                    .version(Version.CURRENT.id)
                    // todo: settings (shards, replicas, ...)
                    // todo: remove type
                    .mapping(MapperService.SINGLE_MAPPING_NAME, mappings());

            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), ClientHelper.DATA_FRAME_ORIGIN, request,
                    ActionListener.<AcknowledgedResponse>wrap(r -> {
                        if (r.isAcknowledged()) {
                            finalListener.onResponse(true);
                        } else {
                            finalListener.onFailure(
                                    new RuntimeException("Error creating data frame index template, request was not acknowledged"));
                        }
                    }, e -> {
                        finalListener.onFailure(new RuntimeException("Error adding data frame index template", e));
                    }), client.admin().indices()::putTemplate);
        } catch (IOException e) {
            finalListener.onFailure(new RuntimeException("Error creating data frame index template", e));
        }
    }

    private static XContentBuilder mappings() throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();

        builder.startObject(MapperService.SINGLE_MAPPING_NAME);
        addMetaInformation(builder);

        // no need to analyze anything, we use the config index as key value store, revisit if we decide to search on it
        builder.field(ENABLED, false);
        // do not allow anything outside of the defined schema
        builder.field(DYNAMIC, "strict");
        // the schema definitions
        builder.startObject(PROPERTIES);
        // overall doc type
        builder.startObject(DOC_TYPE).field(TYPE, KEYWORD).endObject();
        // add the schema for job configurations
        addDataFrameJobConfigMappings(builder);

        // end type
        builder.endObject();
        // end properties
        builder.endObject();
        // end mapping
        builder.endObject();
        return builder;
    }

    private static XContentBuilder addDataFrameJobConfigMappings(XContentBuilder builder) throws IOException {
        return builder
            .startObject(DataFrameField.ID.getPreferredName())
                .field(TYPE, KEYWORD)
            .endObject();
    }

    /**
     * Inserts "_meta" containing useful information like the version into the mapping
     * template.
     *
     * @param builder The builder for the mappings
     * @throws IOException On write error
     */
    private static XContentBuilder addMetaInformation(XContentBuilder builder) throws IOException {
        return builder.startObject("_meta")
                    .field("version", Version.CURRENT)
                .endObject();
    }

    private DataFrameInternalIndex() {
    }
}
