/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Creates aliases for monitoring indexes created by Marvel 2.3+.
 */
public class BackwardsCompatibilityAliasesResource extends HttpResource {
    private static final Logger logger = Loggers.getLogger(BackwardsCompatibilityAliasesResource.class);

    private final TimeValue masterTimeout;

    /**
     * Create a new {@link TemplateHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param masterTimeout Master timeout to use with any request.
     */
    public BackwardsCompatibilityAliasesResource(final String resourceOwnerName, @Nullable final TimeValue masterTimeout) {
        super(resourceOwnerName);
        this.masterTimeout = masterTimeout;
    }

    @Override
    protected boolean doCheckAndPublish(RestClient client) {
        boolean needNewAliases = false;
        XContentBuilder request;
        try {
            Response response = client.performRequest("GET", "/.marvel-es-1-*", Collections.singletonMap("filter_path", "*.aliases"));
            Map<String, Object> indices = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), false);
            request = JsonXContent.contentBuilder();
            request.startObject().startArray("actions");
            for (Map.Entry<String, Object> e : indices.entrySet()) {
                String index = e.getKey();
                // we add a suffix so that it will not collide with today's monitoring index following an upgrade
                String alias = ".monitoring-es-2-" + index.substring(".marvel-es-1-".length()) + "-alias";
                if (false == aliasesForIndex(e.getValue()).contains(alias)) {
                    needNewAliases = true;
                    addAlias(request, index, alias);
                }
            }
            request.endArray().endObject();
        } catch (ResponseException e) {
            int statusCode = e.getResponse().getStatusLine().getStatusCode();

            if (statusCode == RestStatus.NOT_FOUND.getStatus()) {
                logger.debug("no 2.x monitoring indexes found so no need to create backwards compatibility aliases");
                return true;
            }
            logger.error((Supplier<?>) () ->
                    new ParameterizedMessage("failed to check for 2.x monitoring indexes with [{}]", statusCode),
                    e);
            return false;
        } catch (IOException | RuntimeException e) {
            logger.error("failed to check for 2.x monitoring indexes", e);
            return false;
        }

        if (false == needNewAliases) {
            // Hurray! Nothing to do!
            return true;
        }

        /* Looks like we have to create some new aliases. Note that this is a race with all other exporters on other nodes of Elasticsearch
         * targeting this cluster. That is fine because this request is idemopotent, meaning that if it has no work to do it'll just return
         * 200 OK { "acknowledged": true }. */
        try {
            BytesRef bytes = request.bytes().toBytesRef();
            HttpEntity body = new ByteArrayEntity(bytes.bytes, bytes.offset, bytes.length, ContentType.APPLICATION_JSON);
            Response response = client.performRequest("POST", "/_aliases", parameters(), body);
            Map<String, Object> aliasesResponse = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(),
                    false);
            Boolean acked = (Boolean) aliasesResponse.get("acknowledged");
            if (acked == null) {
                logger.error("Unexpected response format from _aliases action {}", aliasesResponse);
                return false;
            }
            return acked;
        } catch (IOException | RuntimeException e) {
            logger.error("failed to create aliases for 2.x monitoring indexes", e);
            return false;
        }
    }

    private Set<?> aliasesForIndex(Object indexInfo) {
        Map<?, ?> info = (Map<?, ?>) indexInfo;
        Map<?, ?> aliases = (Map<?, ?>) info.get("aliases");
        return aliases.keySet();
    }

    /**
     * Parameters to use for all requests.
     */
    Map<String, String> parameters() {
        Map<String, String> parameters = new HashMap<>();
        if (masterTimeout != null) {
            parameters.put("master_timeout", masterTimeout.getStringRep());
        }
        return parameters;
    }

    private void addAlias(XContentBuilder request, String index, String alias) throws IOException {
        request.startObject().startObject("add");
        {
            request.field("index", index);
            request.field("alias", alias);
        }
        request.endObject().endObject();
    }
}
