/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * {@code TemplateHttpResource}s allow the checking and uploading of templates to a remote cluster.
 * <p>
 * There is currently no need to check the response body of the template for consistency, but if we ever make a backwards-compatible change
 * that requires the template to be replaced, then we will need to check for <em>something</em> in the body in order to see if we need to
 * replace the existing template(s).
 */
public class TemplateHttpResource extends PublishableHttpResource {

    private static final Logger logger = Loggers.getLogger(TemplateHttpResource.class);

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
        super(resourceOwnerName, masterTimeout, PublishableHttpResource.NO_BODY_PARAMETERS);

        this.templateName = Objects.requireNonNull(templateName);
        this.template = Objects.requireNonNull(template);
    }

    /**
     * Determine if the current {@linkplain #templateName template} exists.
     */
    @Override
    protected CheckResponse doCheck(final RestClient client) {
        return simpleCheckForResource(client, logger,
                                      "/_template", templateName, "monitoring template",
                                      resourceOwnerName, "monitoring cluster");
    }

    /**
     * Publish the missing {@linkplain #templateName template}.
     */
    @Override
    protected boolean doPublish(final RestClient client) {
        return putResource(client, logger,
                           "/_template", templateName, this::templateToHttpEntity, "monitoring template",
                           resourceOwnerName, "monitoring cluster");
    }

    /**
     * Create a {@link HttpEntity} for the {@link #template}.
     *
     * @return Never {@code null}.
     */
    HttpEntity templateToHttpEntity() {
        return new StringEntity(template.get(), ContentType.APPLICATION_JSON);
    }

}
