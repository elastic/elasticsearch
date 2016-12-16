/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.scheduler.http;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.scheduler.SchedulerConfig;
import org.elasticsearch.xpack.prelert.job.extraction.DataExtractor;
import org.elasticsearch.xpack.prelert.job.extraction.DataExtractorFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HttpDataExtractorFactory implements DataExtractorFactory {

    private static final Logger LOGGER = Loggers.getLogger(HttpDataExtractorFactory.class);

    private final Client client;

    public HttpDataExtractorFactory(Client client) {
        this.client = client;
    }

    @Override
    public DataExtractor newExtractor(SchedulerConfig schedulerConfig, Job job) {
        String timeField = job.getDataDescription().getTimeField();
        ElasticsearchQueryBuilder queryBuilder = new ElasticsearchQueryBuilder(
                stringifyElasticsearchQuery(schedulerConfig.getQuery()),
                stringifyElasticsearchAggregations(schedulerConfig.getAggregations(), schedulerConfig.getAggs()),
                stringifyElasticsearchScriptFields(schedulerConfig.getScriptFields()),
                Boolean.TRUE.equals(schedulerConfig.getRetrieveWholeSource()) ? null : writeListAsJson(job.allFields()),
                        timeField);
        HttpRequester httpRequester = new HttpRequester();
        ElasticsearchUrlBuilder urlBuilder = ElasticsearchUrlBuilder
                .create(schedulerConfig.getIndexes(), schedulerConfig.getTypes(), getBaseUrl());
        return new ElasticsearchDataExtractor(httpRequester, urlBuilder, queryBuilder, schedulerConfig.getScrollSize());
    }

    private String getBaseUrl() {
        NodesInfoResponse nodesInfoResponse = client.admin().cluster().prepareNodesInfo().get();
        TransportAddress address = nodesInfoResponse.getNodes().get(0).getHttp().getAddress().publishAddress();
        String baseUrl = "http://" + address.getAddress() + ":" + address.getPort() + "/";
        LOGGER.info("Base URL: " + baseUrl);
        return baseUrl;
    }

    String stringifyElasticsearchQuery(Map<String, Object> queryMap) {
        String queryStr = writeMapAsJson(queryMap);
        if (queryStr.startsWith("{") && queryStr.endsWith("}")) {
            return queryStr.substring(1, queryStr.length() - 1);
        }
        return queryStr;
    }

    String stringifyElasticsearchAggregations(Map<String, Object> aggregationsMap, Map<String, Object> aggsMap) {
        if (aggregationsMap != null) {
            return writeMapAsJson(aggregationsMap);
        }
        if (aggsMap != null) {
            return writeMapAsJson(aggsMap);
        }
        return null;
    }

    String stringifyElasticsearchScriptFields(Map<String, Object> scriptFieldsMap) {
        if (scriptFieldsMap != null) {
            return writeMapAsJson(scriptFieldsMap);
        }
        return null;
    }

    private static String writeMapAsJson(Map<String, Object> map) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(map);
            return builder.string();
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to convert map to JSON string", e);
        }
    }

    private static String writeListAsJson(List<String> list) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.startArray("a");
            for (String e : list) {
                builder.value(e);
            }
            builder.endArray();
            builder.endObject();
            return builder.string().replace("{\"a\":", "").replace("}", "");
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to convert map to JSON string", e);
        }
    }
}
