/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

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
     * Create a new {@link TemplateHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param masterTimeout Master timeout to use with any request.
     * @param templateName The name of the template (e.g., ".template123").
     */
    public TemplateHttpResource(final String resourceOwnerName, @Nullable final TimeValue masterTimeout, final String templateName) {
        super(resourceOwnerName, masterTimeout, PARAMETERS);

        this.templateName = Objects.requireNonNull(templateName);
    }

    /**
     * Determine if the current {@linkplain #templateName template} exists with a relevant version (&gt;= to expected).
     *
     * @see MonitoringTemplateUtils#LAST_UPDATED_VERSION
     */
    @Override
    protected void doCheck(final RestClient client, final ActionListener<Boolean> listener) {
        versionCheckForResource(
            client,
            listener,
            logger,
            "/_template",
            templateName,
            "monitoring template",
            resourceOwnerName,
            "monitoring cluster",
            XContentType.JSON.xContent(),
            MonitoringTemplateUtils.LAST_UPDATED_VERSION
        );
    }

    /**
     * If a template source is given, then publish the missing {@linkplain #templateName template}. If a template
     * is not provided to be installed, then signal that the resource is not ready until a valid one appears.
     */
    @Override
    protected void doPublish(final RestClient client, final ActionListener<ResourcePublishResult> listener) {
        listener.onResponse(
            ResourcePublishResult.notReady(
                "waiting for remote monitoring cluster to install appropriate template "
                    + "["
                    + templateName
                    + "] (version mismatch or missing)"
            )
        );
    }
}
