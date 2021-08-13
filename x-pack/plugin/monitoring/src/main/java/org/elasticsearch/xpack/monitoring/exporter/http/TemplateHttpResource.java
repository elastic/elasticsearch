/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * {@code TemplateHttpResource}s allow the checking and uploading of templates to a remote cluster.
 * <p>
 * There is currently no need to check the response body of the template for consistency, but if we ever make a backwards-compatible change
 * that requires the template to be replaced, then we will need to check for <em>something</em> in the body in order to see if we need to
 * replace the existing template(s).
 */
public class TemplateHttpResource extends PublishableHttpResource {

    private static final Logger logger = LogManager.getLogger(TemplateHttpResource.class);

    public static final Map<String, String> PARAMETERS;

    static {
        Map<String, String> parameters = new TreeMap<>();
        parameters.put("filter_path", FILTER_PATH_RESOURCE_VERSION);
        PARAMETERS = Collections.unmodifiableMap(parameters);
    }

    /**
     * The name of the template that is sent to the remote cluster.
     */
    private final String templateName;
    /**
     * Provides a fully formed template (e.g., no variables that need replaced).
     */
    private final Supplier<String> template;

    /**
     * Create a new {@link TemplateHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param masterTimeout Master timeout to use with any request.
     * @param templateName The name of the template (e.g., ".template123").
     * @param template The template provider.
     */
    public TemplateHttpResource(final String resourceOwnerName, @Nullable final TimeValue masterTimeout,
                                final String templateName, final Supplier<String> template) {
        super(resourceOwnerName, masterTimeout, PARAMETERS);

        this.templateName = Objects.requireNonNull(templateName);
        this.template = Objects.requireNonNull(template);
    }

    /**
     * Determine if the current {@linkplain #templateName template} exists with a relevant version (&gt;= to expected).
     *
     * @see MonitoringTemplateUtils#LAST_UPDATED_VERSION
     */
    @Override
    protected void doCheck(final RestClient client, final ActionListener<Boolean> listener) {
        versionCheckForResource(client, listener, logger,
                                "/_template", templateName, "monitoring template",
                                resourceOwnerName, "monitoring cluster",
                                XContentType.JSON.xContent(), MonitoringTemplateUtils.LAST_UPDATED_VERSION);
    }

    /**
     * Publish the missing {@linkplain #templateName template}.
     */
    @Override
    protected void doPublish(final RestClient client, final ActionListener<ResourcePublishResult> listener) {
        putResource(client, listener, logger,
                    "/_template", templateName, Collections.emptyMap(), this::templateToHttpEntity, "monitoring template",
                    resourceOwnerName, "monitoring cluster");
    }

    /**
     * Create a {@link HttpEntity} for the {@link #template}.
     *
     * @return Never {@code null}.
     */
     HttpEntity templateToHttpEntity() {
        // the internal representation of a template has type nested under mappings.
        // this uses xContent to help remove the type before sending to the remote cluster
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, template.get())) {
            XContentBuilder builder = JsonXContent.contentBuilder();
            IndexTemplateMetadata.Builder.removeType(IndexTemplateMetadata.Builder.fromXContent(parser, templateName), builder);
            return new StringEntity(BytesReference.bytes(builder).utf8ToString(), ContentType.APPLICATION_JSON);
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot serialize template [" + templateName + "] for monitoring export", ex);
        }
    }

}
