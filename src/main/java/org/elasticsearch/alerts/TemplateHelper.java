/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 */
public class TemplateHelper extends AbstractComponent {

    private final MetaDataIndexTemplateService indexTemplateService;
    private final TransportPutIndexTemplateAction transportPutIndexTemplateAction;

    @Inject
    public TemplateHelper(Settings settings, MetaDataIndexTemplateService indexTemplateService, TransportPutIndexTemplateAction transportPutIndexTemplateAction) {
        super(settings);
        this.indexTemplateService = indexTemplateService;
        this.transportPutIndexTemplateAction = transportPutIndexTemplateAction;
    }

    /**
     * Checks if the template with the specified name exists and has the expected version.
     * If that isn't the case then the template from the classpath will be uploaded to the cluster
     */
    public void checkAndUploadIndexTemplate(ClusterState state, final String templateName) {
        final byte[] template;
        try {
            InputStream is = AlertsStore.class.getResourceAsStream("/" + templateName + ".json");
            if (is == null) {
                throw new FileNotFoundException("Resource [/" + templateName + ".json] not found in classpath");
            }
            template = Streams.copyToByteArray(is);
            is.close();
        } catch (IOException e) {
            // throwing an exception to stop exporting process - we don't want to send data unless
            // we put in the template for it.
            throw new RuntimeException("failed to load " + templateName + ".json", e);
        }

        try {
            int expectedVersion = parseIndexVersionFromTemplate(template);
            if (expectedVersion < 0) {
                throw new RuntimeException("failed to find an index version in pre-configured index template");
            }

            IndexTemplateMetaData templateMetaData = state.metaData().templates().get(templateName);
            if (templateMetaData != null) {
                int foundVersion = templateMetaData.getSettings().getAsInt("alerts.template_version", -1);
                if (foundVersion < 0) {
                    logger.warn("found an existing index template [{}] but couldn't extract it's version. leaving it as is.", templateName);
                    return;
                } else if (foundVersion >= expectedVersion) {
                    logger.info("accepting existing index template [{}] (version [{}], needed [{}])", templateName, foundVersion, expectedVersion);
                    return;
                } else {
                    logger.info("replacing existing index template [{}] (version [{}], needed [{}])", templateName, foundVersion, expectedVersion);
                }
            } else {
                logger.info("Adding index template [{}], because none was found", templateName);
            }
            MetaDataIndexTemplateService.PutRequest putRequest = new MetaDataIndexTemplateService.PutRequest("alerts-template", templateName);
            XContent xContent = XContentFactory.xContent(template, 0, template.length);
            try (XContentParser parser = xContent.createParser(template, 0, template.length)) {
                String currentFieldName = null;
                XContentParser.Token token = parser.nextToken();
                assert token == XContentParser.Token.START_OBJECT;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    switch (token) {
                        case FIELD_NAME:
                            currentFieldName = parser.currentName();
                            break;
                        case START_OBJECT:
                            switch (currentFieldName) {
                                case "settings":
                                    XContentBuilder settingsBuilder = jsonBuilder();
                                    settingsBuilder.copyCurrentStructure(parser);
                                    String source = settingsBuilder.string();
                                    putRequest.settings(ImmutableSettings.settingsBuilder().loadFromSource(source).build());
                                    break;
                                case "mappings":
                                    Map<String, String> mappingSource = new HashMap<>();
                                    String currentMappingFieldName = null;
                                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                        switch (token) {
                                            case FIELD_NAME:
                                                currentMappingFieldName = parser.currentName();
                                                break;
                                            case START_OBJECT:
                                                XContentBuilder mappingsBuilder = jsonBuilder();
                                                mappingsBuilder.copyCurrentStructure(parser);
                                                mappingSource.put(currentMappingFieldName, mappingsBuilder.string());
                                                break;
                                        }
                                    }
                                    putRequest.mappings(mappingSource);
                                    break;
                                default:
                                    throw new ElasticsearchIllegalArgumentException("Unsupported token [" + token + "]");
                            }
                            break;
                        case VALUE_STRING:
                            if ("template".equals(currentFieldName)) {
                                putRequest.template(parser.textOrNull());
                            } else {
                                throw new ElasticsearchIllegalArgumentException("Unsupported field [" + currentFieldName + "]");
                            }
                            break;
                        case VALUE_NUMBER:
                            if ("order".equals(currentFieldName)) {
                                putRequest.order(parser.intValue());
                            } else {
                                throw new ElasticsearchIllegalArgumentException("Unsupported field [" + currentFieldName + "]");
                            }
                            break;
                        default:
                            throw new ElasticsearchIllegalArgumentException("Unsupported token [" + token + "]");
                    }
                }
            }
            indexTemplateService.putTemplate(putRequest, new MetaDataIndexTemplateService.PutListener() {
                @Override
                public void onResponse(MetaDataIndexTemplateService.PutResponse response) {
                    logger.info("Adding template [{}] was successful", templateName);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.debug("failed to add template [{}]", t, templateName);
                }
            });
        } catch (IOException e) {
            // if we're not sure of the template, we can't send data... re-raise exception.
            throw new RuntimeException("failed to load/verify index template", e);
        }
    }

    private static int parseIndexVersionFromTemplate(byte[] template) throws UnsupportedEncodingException {
        Pattern versionRegex = Pattern.compile("alerts.template_version\"\\s*:\\s*\"?(\\d+)\"?");
        Matcher matcher = versionRegex.matcher(new String(template, "UTF-8"));
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            return -1;
        }
    }

}
