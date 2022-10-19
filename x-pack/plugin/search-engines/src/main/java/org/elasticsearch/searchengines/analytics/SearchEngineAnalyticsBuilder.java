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

// Path forward:
// 0. Check if data stream exists. If yes, do nothing else.
// 1. Check if component template exists. If not, create it.
// 2. Check if template exists. If not, create it.
// 3. Check if data stream exists. If not, create it.
public class SearchEngineAnalyticsBuilder {

    private static final Logger logger = LogManager.getLogger(SearchEngineAnalyticsBuilder.class);

    public static void ensureDataStreamExists(String dataStreamName, Client client) {
        if (dataStreamExists(dataStreamName, client) == false) {
            uglyUpsertDataStream(dataStreamName, client);
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

    private static void createDataStreamAndTemplate(String dataStreamName, Client client) {

        upsertDataStreamTemplate(dataStreamName, client);

        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client.execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest, new ActionListener<AcknowledgedResponse>() {
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

    private static void createDataStream(String dataStreamName, Client client) {

        logger.info("Creating data stream " + dataStreamName);

        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client.execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest, new ActionListener<AcknowledgedResponse>() {
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

    private static void uglyUpsertDataStream(String dataStreamName, Client client) {

        try {
            // Component template
            String mappingComponentTemplateName = dataStreamName + "_mappings";
            if (componentTemplateExists(mappingComponentTemplateName, client)) {
                logger.info(
                    "Component template "
                        + mappingComponentTemplateName
                        + " already exists, creating data stream template for "
                        + dataStreamName
                );
                upsertDataStreamTemplate(dataStreamName, client);
            } else {
                logger.info("Creating component template " + mappingComponentTemplateName);
                CompressedXContent mappings = new CompressedXContent(componentTemplateJson());
                ComponentTemplate componentTemplate = new ComponentTemplate(new Template(null, mappings, null), null, null);
                PutComponentTemplateAction.Request componentTemplateRequest = new PutComponentTemplateAction.Request(
                    mappingComponentTemplateName
                ).componentTemplate(componentTemplate);
                // .create(true);

                client.execute(PutComponentTemplateAction.INSTANCE, componentTemplateRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        upsertDataStreamTemplate(dataStreamName, client);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("error creating component template", e);
                    }
                });

            }

        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }

    }

    private static void upsertDataStreamTemplate(String dataStreamName, Client client) {

        String templateName = dataStreamName + "_template";
        String mappingComponentTemplateName = dataStreamName + "_mappings";
        String[] componentTemplateNames = new String[] { mappingComponentTemplateName };
        // createComponentTemplates(dataStreamName, client);

        // TODO - where do you set "data_stream" in this request?
        ComposableIndexTemplate template = new ComposableIndexTemplate(
            List.of(dataStreamName + "*"),
            null,
            Arrays.asList(componentTemplateNames),
            500L,
            1L,
            Map.of("description", "Template for " + dataStreamName + " analytics time series data")
        );

        logger.info("Creating data stream template " + templateName);

        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(templateName);
        // request.create(true);
        request.indexTemplate(template);

        logger.info(template.toString());

        client.execute(PutComposableIndexTemplateAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                // No action required
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

    // TODO org.elasticsearch.ElasticsearchParseException: unknown key [data_stream] in the template
    private static XContentBuilder buildTemplateRequest(String dataStreamName, String[] componentTemplateNames) {
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE)) {
            builder.startObject();
            builder.field("index_patterns", new String[] { dataStreamName + "*" });
            builder.field("data_stream");
            builder.startObject();
            builder.endObject();
            builder.field("composed_of", componentTemplateNames);
            builder.field("priority", 500);
            builder.startObject("_meta");
            builder.field("description", "Template for " + dataStreamName + " time series data");
            builder.endObject();
            builder.endObject();

            return builder;
        } catch (IOException e) {
            throw new ElasticsearchException("error building template request", e);
        }
    }

    private static void createComponentTemplates(String mappingComponentTemplateName, Client client) {
        createMappingComponentTemplate(mappingComponentTemplateName, client);
    }

    private static boolean componentTemplateExists(String mappingComponentTemplateName, Client client) {
        final boolean[] exists = { false };
        GetComponentTemplateAction.Request request = new GetComponentTemplateAction.Request(mappingComponentTemplateName);
        client.execute(GetComponentTemplateAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(GetComponentTemplateAction.Response response) {
                logger.info("Component Templates: " + response.getComponentTemplates().keySet());
                exists[0] = response.getComponentTemplates().containsKey(mappingComponentTemplateName);
            }

            @Override
            public void onFailure(Exception e) {
                // Do nothing
            }
        });
        return exists[0];
    }

    private static void createMappingComponentTemplate(String mappingComponentTemplateName, Client client) {
        logger.info("Creating component template " + mappingComponentTemplateName);

        try {
            CompressedXContent mappings = new CompressedXContent(componentTemplateJson());
            ComponentTemplate componentTemplate = new ComponentTemplate(new Template(null, mappings, null), null, null);
            PutComponentTemplateAction.Request componentTemplateRequest = new PutComponentTemplateAction.Request(
                mappingComponentTemplateName
            ).componentTemplate(componentTemplate);
            // .create(true);

            client.execute(PutComponentTemplateAction.INSTANCE, componentTemplateRequest, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    // no action required
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("error creating component template", e);
                }
            });

        } catch (IOException e) {
            logger.error("error creating component template", e);
        }
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
