/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.xsearch.analytics;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

public class XSearchAnalyticsBuilder {

    private static final Logger logger = LogManager.getLogger(XSearchAnalyticsBuilder.class);

    public static void ensureDataStreamExists(String dataStreamName, NodeClient client) {
        if (dataStreamExists(dataStreamName, client) == false) {
            createDataStream(dataStreamName, client);
        }
    }

    private static boolean dataStreamExists(String dataStreamName, NodeClient client) {
        final boolean[] dataStreamExists = { false };
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
        client.execute(GetDataStreamAction.INSTANCE, getDataStreamRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetDataStreamAction.Response response) {
                dataStreamExists[0] = response.getDataStreams()
                    .stream()
                    .map(GetDataStreamAction.Response.DataStreamInfo::getDataStream)
                    .filter(dataStream -> dataStream.getName().equals(dataStreamName))
                    .count() == 1;
            }

            @Override
            public void onFailure(Exception e) {
                throw new ElasticsearchException("Could not retrieve DataStream " + dataStreamName, e);
            }
        });
        return dataStreamExists[0];
    }

    private static void createDataStream(String dataStreamName, NodeClient client) {
        upsertDataStreamTemplate(dataStreamName, client);
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client.execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest, new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                logger.info("Created DataStream " + dataStreamName);
            }

            @Override
            public void onFailure(Exception e) {
                throw new ElasticsearchException("Failed to create DataStream " + dataStreamName, e);
            }
        });

    }

    private static void upsertDataStreamTemplate(String dataStreamName, NodeClient client) {
        String templateName = dataStreamName + "_template";
        String[] componentTemplateNames = createComponentTemplates(dataStreamName, client);
        ActionRequest templateRequest = client.admin()
            .indices()
            .preparePutTemplate(templateName)
            .setSource(buildTemplateRequest(dataStreamName, componentTemplateNames))
            .request();
        client.execute(PutIndexTemplateAction.INSTANCE, templateRequest, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                // No action required
            }

            @Override
            public void onFailure(Exception e) {
                throw new ElasticsearchException("template error for DataStream " + dataStreamName, e);
            }
        });
    }

    private static XContentBuilder buildTemplateRequest(String dataStreamName, String[] componentTemplateNames) {
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE)) {
            builder.startObject();
            builder.field("index_patterns", new String[] { dataStreamName + "*" });
            builder.startObject("data_stream");
            builder.endObject();
            builder.field("composed_of", componentTemplateNames);
            builder.field("priority", 500);
            builder.startObject("_meta");
            builder.field("description", "Template for " + dataStreamName + " time series data");
            builder.endObject();
            builder.endObject();

            return builder;
        } catch (IOException e) {
            throw new ElasticsearchException("template error for data stream " + dataStreamName, e);
        }
    }

    private static String[] createComponentTemplates(String dataStreamName, NodeClient client) {

        String mappingComponentTemplateName = dataStreamName + "_mappings";

        try (XContentBuilder componentTemplateBuilder = buildMappingComponentTemplateRequest()) {
            CompressedXContent mappings = new CompressedXContent(componentTemplateBuilder.toString());
            ComponentTemplate componentTemplate = new ComponentTemplate(new Template(null, mappings, null), null, null);
            PutComponentTemplateAction.Request componentTemplateRequest = new PutComponentTemplateAction.Request(
                mappingComponentTemplateName
            ).componentTemplate(componentTemplate).create(true);

            client.execute(PutComponentTemplateAction.INSTANCE, componentTemplateRequest, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    // Do nothing
                }

                @Override
                public void onFailure(Exception e) {
                    throw new ElasticsearchException("component template error for data stream " + dataStreamName, e);
                }
            });

        } catch (IOException e) {

        }

        return new String[] { mappingComponentTemplateName };
    }

    private static XContentBuilder buildMappingComponentTemplateRequest() {
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE)) {
            builder.startObject();
            builder.startObject("template");
            builder.field("mappings");
            builder.startObject("properties");
            builder.startObject("@timestamp");
            builder.field("type", "date");
            builder.field("format", "date_optional_time||epoch_millis");
            builder.endObject();
            builder.startObject("query");
            builder.field("type", "keyword");
            builder.endObject();
            builder.endObject();
            builder.endObject();
            builder.endObject();

            return builder;
        } catch (IOException e) {
            throw new ElasticsearchException("template error", e);
        }
    }
}
