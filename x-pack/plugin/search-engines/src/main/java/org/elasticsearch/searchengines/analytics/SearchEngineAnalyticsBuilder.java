/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.searchengines.analytics;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The SearchRequestAnalyticsBuilder tries to ensure that an analytics data stream exists for the specified
 * {@link org.elasticsearch.cluster.metadata.SearchEngine} if an analytics collection is specified.
 * If for some reason a data stream cannot be created, we log a message but don't throw an error, and allow
 * engine requests to continue.
 */
public class SearchEngineAnalyticsBuilder {

    private static final Logger logger = LogManager.getLogger(SearchEngineAnalyticsBuilder.class);
    private static final String ANALYTICS_TEMPLATE_POSTFIX = "_template";
    private static final String COMPONENT_MAPPING_TEMPLATE_POSTFIX = "_mappings";

    public static void ensureDataStreamExists(String dataStreamName, Client client) {
        if (dataStreamExists(dataStreamName, client) == false) {
            upsertDataStreamAndRequiredTemplates(dataStreamName, client);
        }
    }

    private static boolean dataStreamExists(String dataStreamName, Client client) {
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
                // Do nothing
            }
        });
        return dataStreamExists[0];
    }

    /**
     * Assumes that required templates already exist.
     */
    private static void createDataStream(String dataStreamName, Client client) {

        logger.info("Creating data stream " + dataStreamName);
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client.execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                logger.info("Data stream [{}] created to log analytics for engine", dataStreamName);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Could not create data stream [{}], analytics will not be recorded for this engine", dataStreamName, e);
            }
        });

    }

    private static void upsertDataStreamAndRequiredTemplates(String dataStreamName, Client client) {

        try {
            // Component template
            String mappingComponentTemplateName = dataStreamName + COMPONENT_MAPPING_TEMPLATE_POSTFIX;
            String[] componentTemplateNames = new String[] { mappingComponentTemplateName };
            if (componentTemplateExists(mappingComponentTemplateName, client)) {
                upsertDataStreamTemplate(dataStreamName, componentTemplateNames, client);
            } else {
                logger.info("Creating component template " + mappingComponentTemplateName);
                ComponentTemplate mappingComponentTemplate = new ComponentTemplate(
                    new Template(null, new CompressedXContent(componentTemplateJson()), null),
                    null,
                    null
                );
                PutComponentTemplateAction.Request mappingComponentTemplateRequest = new PutComponentTemplateAction.Request(
                    mappingComponentTemplateName
                ).componentTemplate(mappingComponentTemplate);
                client.execute(PutComponentTemplateAction.INSTANCE, mappingComponentTemplateRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        upsertDataStreamTemplate(dataStreamName, componentTemplateNames, client);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("error creating component template", e);
                    }
                });
            }
        } catch (IOException e) {
            logger.error("Error upserting data stream", e);
        }

    }

    private static void upsertDataStreamTemplate(String dataStreamName, String[] componentTemplateNames, Client client) {

        String templateName = dataStreamName + ANALYTICS_TEMPLATE_POSTFIX;
        logger.info("Creating analytics data stream template " + templateName + " for data stream " + dataStreamName);

        List<String> indexPatterns = List.of(dataStreamName + "*");
        ComposableIndexTemplate template = new ComposableIndexTemplate(
            indexPatterns,
            null,
            Arrays.asList(componentTemplateNames),
            500L,
            1L,
            Map.of("description", "Template for " + dataStreamName + " analytics time series data"),
            new ComposableIndexTemplate.DataStreamTemplate()
        );

        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(templateName).indexTemplate(
            template
        );
        client.execute(PutComposableIndexTemplateAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                if (dataStreamExists(dataStreamName, client) == false) {
                    createDataStream(dataStreamName, client);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("could not create template", dataStreamName, e);
            }
        });
    }

    private static boolean componentTemplateExists(String mappingComponentTemplateName, Client client) {
        final boolean[] exists = { false };
        GetComponentTemplateAction.Request request = new GetComponentTemplateAction.Request(mappingComponentTemplateName);
        client.execute(GetComponentTemplateAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(GetComponentTemplateAction.Response response) {
                exists[0] = response.getComponentTemplates().containsKey(mappingComponentTemplateName);
            }

            @Override
            public void onFailure(Exception e) {
                // Do nothing
            }
        });
        return exists[0];
    }

    // TODO - replace this with XContentBuilder or something else more structured
    private static String componentTemplateJson() {
        return """
            {
                  "properties": {
                    "@timestamp": {
                      "type": "date",
                      "format": "date_optional_time||epoch_millis"
                    },
                    "query": {
                      "type": "keyword"
                    }
                  }
                }
            """;
    }

    private static XContentBuilder buildMappingComponentTemplateRequest() {
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE)) {
            builder.startObject();
            builder.field("properties");
            builder.startObject();
            builder.field("@timestamp");
            builder.startObject();
            builder.field("type", "date");
            builder.field("format", "date_optional_time||epoch_millis");
            builder.endObject();
            builder.field("query");
            builder.startObject();
            builder.field("type", "keyword");
            builder.endObject();
            builder.endObject();
            builder.endObject();

            return builder;
        } catch (IOException e) {
            throw new ElasticsearchException("could not build xcontent for component template mappings", e);
        }
    }
}
