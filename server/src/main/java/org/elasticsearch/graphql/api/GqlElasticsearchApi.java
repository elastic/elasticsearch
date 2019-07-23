/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.graphql.api;

import static org.elasticsearch.graphql.api.GqlApiUtils.futureToListener;
import static org.elasticsearch.graphql.api.GqlApiUtils.log;
import static org.elasticsearch.graphql.api.GqlApiUtils.getSomeMapKey;
import static org.elasticsearch.graphql.api.GqlApiUtils.executeRestHandler;
import static org.elasticsearch.graphql.api.GqlApiUtils.executeAction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.client.node.NodeClient;
import static org.elasticsearch.rest.RestRequest.Method.GET;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.action.cat.RestIndicesAction;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class GqlElasticsearchApi implements GqlApi {
    private static final Logger logger = LogManager.getLogger(GqlElasticsearchApi.class);

    NodeClient client;

    public GqlElasticsearchApi(NodeClient client) {
        this.client = client;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Map<String, Object>> getInfo() throws Exception {
        CompletableFuture<Map<String, Object>> future = executeAction(client, MainAction.INSTANCE, new MainRequest());

        return future
            .thenApply(obj -> {
                obj.put("clusterName", obj.get("cluster_name"));
                obj.put("clusterUuid", obj.get("cluster_uuid"));

                Object maybeMap = obj.get("version");
                if (maybeMap instanceof Map) {
                    Map<String, Object> version = (Map<String, Object>) maybeMap;
                    version.put("buildFlavor", version.get("build_flavor"));
                    version.put("buildType", version.get("build_type"));
                    version.put("buildHash", version.get("build_hash"));
                    version.put("buildDate", version.get("build_date"));
                    version.put("buildSnapshot", version.get("build_snapshot"));
                    version.put("lucene", version.get("lucene_version"));
                    version.put("minimumWireCompatibilityVersion", version.get("minimum_wire_compatibility_version"));
                    version.put("minimumIndexCompatibilityVersion", version.get("minimum_index_compatibility_version"));
                }

                return obj;
            });
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<List<Object>> getIndices() throws Exception {
        return executeRestHandler(client, RestIndicesAction.INSTANCE, GET, "/_cat/indices?format=json");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> transformIndexData(Map<String, Object> obj) throws Exception {
        String indexName = getSomeMapKey(obj);
        Map<String, Object> data = (Map<String, Object>) obj.get(indexName);
        Map<String, Object> indexSettings = (Map<String, Object>) data.get("settings");
        indexSettings = (Map<String, Object>) indexSettings.get("index");

        /*
                "number_of_shards": "1",
                "auto_expand_replicas": "0-1",
                "provided_name": ".security-7",
                "format": "6",
                "creation_date": "1563885155564",
                "analysis": {
                    "filter": {
                        "email": {
                            "type": "pattern_capture",
                            "preserve_original": "true",
                            "patterns": [
                                "([^@]+)",
                                "(\\p{L}+)",
                                "(\\d+)",
                                "@(.+)"
                            ]
                        }
                    },
                    "analyzer": {
                        "email": {
                            "filter": [
                                "email",
                                "lowercase",
                                "unique"
                            ],
                            "tokenizer": "uax_url_email"
                        }
                    }
                },
                "priority": "1000",
                "number_of_replicas": "0",
                "uuid": "lvvijma8RD6D-TJrtPMVUg",
                "version": {
                    "created": "8000099"
                }
         */
        indexSettings.put("numberOfShards", indexSettings.get("number_of_shards"));
        indexSettings.remove("number_of_shards");
        indexSettings.put("autoExpandReplicas", indexSettings.get("auto_expand_replicas"));
        indexSettings.remove("auto_expand_replicas");
        indexSettings.put("providerName", indexSettings.get("provided_name"));
        indexSettings.remove("provided_name");
        indexSettings.put("creationDate", indexSettings.get("creation_date"));
        indexSettings.remove("creation_date");
        indexSettings.put("numberOfReplicas", indexSettings.get("number_of_replicas"));
        indexSettings.remove("number_of_replicas");

        indexSettings.put("mappings", data.get("mappings"));

        return indexSettings;
    }

    private static Function<Map<String, Object>, Map<String, Object>> mapIndexData = obj -> {
        try {
            return transformIndexData(obj);
        } catch (Exception e) {
            return null;
        }
    };

    @Override
    public CompletableFuture<Map<String, Object>> getIndex(String indexName) throws Exception {
        logger.info("getIndex [indexName = {}]", indexName);

        String[] indices = { indexName };

        final GetIndexRequest getIndexRequest = new GetIndexRequest()
            .indices(indices);
        CompletableFuture<GetIndexResponse> future = new CompletableFuture<GetIndexResponse>();
        client.admin().indices().getIndex(getIndexRequest, futureToListener(future));

        return future
            .thenApply(GqlApiUtils::toMapSafe)
            .thenApply(mapIndexData)
            .thenApply(log(logger, "getIndex"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> transformDocumentData(Map<String, Object> obj) throws Exception {
        /*
            {_index=twitter, _type=_doc, _id=1, _version=1, _seq_no=0, _primary_term=1, found=true, _source={
       │          "user" : "kimchy",
       │          "post_date" : "2009-11-15T14:12:12",
       │          "message" : "trying out Elasticsearch"
   │        }
         */

        obj.put("indexName", obj.get("_index"));
        obj.remove("_index");

        obj.put("type", obj.get("_type"));
        obj.remove("_type");

        obj.put("id", obj.get("_id"));
        obj.remove("_id");

        obj.put("version", obj.get("_version"));
        obj.remove("_version");

        obj.put("sequenceNumber", obj.get("_seq_no"));
        obj.remove("_seq_no");

        obj.put("primaryTerm", obj.get("_primary_term"));
        obj.remove("_primary_term");

        Map<String, Object> source = XContentHelper.convertToMap(JsonXContent.jsonXContent, (String) obj.get("_source"), false);
        obj.put("source", source);
        obj.remove("_source");

        return obj;
    }

    private static Function<Map<String, Object>, Map<String, Object>> mapDocumentData = obj -> {
        try {
            return transformDocumentData(obj);
        } catch (Exception e) {
            return null;
        }
    };

    @Override
    public CompletableFuture<Map<String, Object>> getDocument(String indexName, String documentId) throws Exception {
        logger.info("getDocument [indexName = {}, documentId]", indexName, documentId);

        GetRequest request = new GetRequest(indexName, documentId);
        request.fetchSourceContext(new FetchSourceContext(true));
        CompletableFuture<GetResponse> future = new CompletableFuture<GetResponse>();
        client.get(request, futureToListener(future));

        return future
            .thenApply(GqlApiUtils::toMapSafe)
            .thenApply(mapDocumentData)
            .thenApply(log(logger, "getDocument"));

    }
}
