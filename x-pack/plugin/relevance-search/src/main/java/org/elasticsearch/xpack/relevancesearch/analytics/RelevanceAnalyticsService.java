/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.analytics;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class RelevanceAnalyticsService implements ClusterStateListener {

    private static final String TEMPLATE_NAME = "ent-search-relevance-search-analytics";

    private final Client client;

    private final ClusterService clusterService;

    private static final Logger logger = LogManager.getLogger(RelevanceAnalyticsService.class);

    public RelevanceAnalyticsService(final Client client, final ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
        clusterService.addListener(this);
    }

    public void logEvent(RelevanceSearchEvent relevanceSearchEvent) {
        throw new UnsupportedOperationException("Not yet");
    }

    // ----------------- Methods to set up the datastream and associated templates
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait for state recovered
            return;
        }

        ensureTemplateExists(this.client);
        ensureDataStreamExists(this.client);
        this.clusterService.removeListener(this);
    }

    private static void ensureDataStreamExists(Client client) {
        throw new UnsupportedOperationException("Not yet");
    }

    private static void ensureTemplateExists(Client client) {
        if (templateExists(client) == false) {
            createTemplate(client);
        }
    }

    private static boolean templateExists(Client client) {
        return client.execute(GetComponentTemplateAction.INSTANCE, new GetComponentTemplateAction.Request(TEMPLATE_NAME))
            .actionGet()
            .getComponentTemplates()
            .keySet()
            .toArray(String[]::new).length > 0;
    }

    private static void createTemplate(Client client) {
        try {
            ComponentTemplate template = new ComponentTemplate(
                // TODO determine what the value of version should be
                // TODO pass in values for settings, aliases
                new Template(null, CompressedXContent.fromJSON(getTemplateBody().toString()), null),
                1L,
                null
            );
            PutComponentTemplateAction.Request request = new PutComponentTemplateAction.Request(TEMPLATE_NAME).componentTemplate(template);
            client.execute(PutComponentTemplateAction.INSTANCE, request).actionGet();
        } catch (IOException e) {
            // TODO - Raise the appropriate error here
            logger.error("Exception creating template " + TEMPLATE_NAME, e);
        }

    }

    /**
     * {
     *   "index_patterns": [".ent-search-relevance-search-analytics*"],
     *   "version": <version>,
     *   "_meta": {
     *     "description": "Template used by Enterprise Search to record relevance search analytics",
     *     "managed": true
     *   },
     *   "data_stream": { },
     *   "template": {
     *     "mappings": {
     *       "_source": {
     *         "enabled": true
     *       },
     *       "properties": {
     *         "query": {
     *           "type": "text",
     *           "fields": {
     *             "keyword": {
     *               "type": "keyword"
     *             }
     *           }
     *         },
     *         "relevance_settings_id": {
     *           "type": "keyword"
     *         },
     *         "curations_id": {
     *           "type": "keyword"
     *         },
     *         "@timestamp": {
     *           "type": "date",
     *           "format": "EEE MMM dd HH:mm:ss Z yyyy"
     *         }
     *       }
     *     },
     *     "aliases": {
     *       ".ent-search-relevance-search-analytics": { }
     *     }
     *   }
     * }
     */
    // TODO it looks like we need to break this up into settings, mappings and aliases
    // TODO determine if we want to go this route, or pull the file from src/main/resources
    private static XContentBuilder getTemplateBody() {
        try {
            return jsonBuilder().startObject()
                .stringListField("index_patterns", List.of(".ent-search-relevance-search-analytics*"))
                .field("version", Version.CURRENT)
                .startObject("_meta")
                .field("description", "Template used by Enterprise Search to record relevance search analytics")
                .field("managed", true)
                .endObject() // end _meta object
                .startObject("data_stream")
                .endObject() // end data_stream object
                .startObject("template")
                .startObject("mappings")
                .startObject("_source")
                .field("enabled", true)
                .endObject() // end _source
                .startObject("properties")
                .startObject("query")
                .field("type", "text")
                .startObject("fields")
                .startObject("keyword")
                .field("type", "keyword")
                .endObject() // end keyword
                .endObject() // end fields
                .endObject() // end query
                .startObject("relevance_settings_id")
                .field("type", "keyword")
                .endObject() // end relevance_settings_id
                .startObject("curations_id")
                .field("type", "keyword")
                .endObject() // end curations_id
                .startObject("@timestamp")
                .field("type", "date")
                .field("format", "EEE MMM dd HH:mm:ss Z yyyy")
                .endObject() // end @timestamp
                .endObject() // end properties
                .endObject() // end mappings
                .startObject("aliases")
                .startObject(".ent-search-relevance-search-analytics")
                .endObject() // end .ent-search-relevance-search-analytics
                .endObject() // end aliases
                .endObject() // end template
                .endObject(); // end json
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build component template for " + TEMPLATE_NAME, e);
        }
    }
}
