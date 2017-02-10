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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Collections;
import java.util.Objects;

import static org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils.DATA_INDEX;

/**
 * {@linkplain DataTypeMappingHttpResource}s allow the checking and adding of index mapping's for new types that did not exist in previous
 * versions.
 * <p>
 * This allows the use of Monitoring's REST endpoint to publish Kibana data to the data index even if the "kibana" type did not
 * exist in their existing index mapping (e.g., they started with an early alpha release). Additionally, this also enables future types to
 * be added without issue.
 * <p>
 * The root need for this is because the index mapping started with an index setting: "index.mapper.dynamic" set to false. This prevents
 * new types from being dynamically added, which is obviously needed as new components (e.g., Kibana and Logstash) are monitored.
 * Unfortunately, this setting cannot be flipped without also closing and reopening the index, so the fix is to manually add any new types.
 */
public class DataTypeMappingHttpResource extends PublishableHttpResource {

    private static final Logger logger = Loggers.getLogger(DataTypeMappingHttpResource.class);

    /**
     * The name of the type that is created in the mappings on the remote cluster.
     */
    private final String typeName;

    /**
     * Create a new {@link DataTypeMappingHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name
     * @param masterTimeout Master timeout to use with any request.
     * @param typeName The name of the mapping type (e.g., "kibana").
     */
    public DataTypeMappingHttpResource(final String resourceOwnerName, @Nullable final TimeValue masterTimeout,
                                       final String typeName) {
        // we need to inspect the mappings, so we don't use filter_path to get rid of them
        super(resourceOwnerName, masterTimeout, Collections.emptyMap());

        this.typeName = Objects.requireNonNull(typeName);
    }

    /**
     * Determine if the current {@linkplain #typeName type} exists.
     */
    @Override
    protected CheckResponse doCheck(final RestClient client) {
        final Tuple<CheckResponse, Response> resource =
                checkForResource(client, logger,
                                 "/" + DATA_INDEX + "/_mapping", typeName, "monitoring mapping type",
                                 resourceOwnerName, "monitoring cluster");

        // depending on the content, we need to flip the actual response
        CheckResponse checkResponse = resource.v1();

        if (checkResponse == CheckResponse.EXISTS && resource.v2().getEntity().getContentLength() <= 2) {
            // it "exists" if the index exists at all; it doesn't guarantee that the mapping exists
            // the content will be "{}" if no mapping exists
            checkResponse = CheckResponse.DOES_NOT_EXIST;
        } else if (checkResponse == CheckResponse.DOES_NOT_EXIST) {
            // DNE indicates that the entire index is missing, which means the template will create it; we only add types!
            checkResponse = CheckResponse.EXISTS;
        }

        return checkResponse;
    }

    /**
     * Add the current {@linkplain #typeName type} to the index's mappings.
     */
    @Override
    protected boolean doPublish(final RestClient client) {
        // this could be a class-level constant, but it does not need to live the entire duration of ES; only the few times it is used
        final HttpEntity disabledEntity = new StringEntity("{\"enabled\":false}", ContentType.APPLICATION_JSON);

        return putResource(client, logger,
                           "/" + DATA_INDEX + "/_mapping", typeName, () -> disabledEntity, "monitoring mapping type",
                           resourceOwnerName, "monitoring cluster");
    }

}
