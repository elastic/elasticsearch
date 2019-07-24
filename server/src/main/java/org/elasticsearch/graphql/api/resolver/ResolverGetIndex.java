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

package org.elasticsearch.graphql.api.resolver;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.graphql.api.GqlApiUtils;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.elasticsearch.graphql.api.GqlApiUtils.*;

public class ResolverGetIndex {

    /**
     * Transform from data returned by ES
     *
     * ```json
     *              {
     *                 "number_of_shards": "1",
     *                 "auto_expand_replicas": "0-1",
     *                 "provided_name": ".security-7",
     *                 "format": "6",
     *                 "creation_date": "1563885155564",
     *                 "analysis": {
     *                     "filter": {
     *                         "email": {
     *                             "type": "pattern_capture",
     *                             "preserve_original": "true",
     *                             "patterns": [
     *                                 "([^@]+)",
     *                                 "(\\p{L}+)",
     *                                 "(\\d+)",
     *                                 "@(.+)"
     *                             ]
     *                         }
     *                     },
     *                     "analyzer": {
     *                         "email": {
     *                             "filter": [
     *                                 "email",
     *                                 "lowercase",
     *                                 "unique"
     *                             ],
     *                             "tokenizer": "uax_url_email"
     *                         }
     *                     }
     *                 },
     *                 "priority": "1000",
     *                 "number_of_replicas": "0",
     *                 "uuid": "lvvijma8RD6D-TJrtPMVUg",
     *                 "version": {
     *                     "created": "8000099"
     *                 }
     *              }
     * ```
     *
     * to what GraphQL expects.
     *
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> transformIndexData(Map<String, Object> obj) throws Exception {
        String indexName = getSomeMapKey(obj);
        Map<String, Object> data = (Map<String, Object>) obj.get(indexName);
        Map<String, Object> indexSettings = (Map<String, Object>) data.get("settings");
        indexSettings = (Map<String, Object>) indexSettings.get("index");


        indexSettings.put("name", indexName);
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

    @SuppressWarnings("unchecked")
    public static CompletableFuture<Map<String, Object>> exec(NodeClient client, String indexName) throws Exception {
        String[] indices = { indexName };

        final GetIndexRequest getIndexRequest = new GetIndexRequest()
            .indices(indices);
        CompletableFuture<GetIndexResponse> future = new CompletableFuture<GetIndexResponse>();
        client.admin().indices().getIndex(getIndexRequest, futureToListener(future));

        return future
            .thenApply(GqlApiUtils::toMapSafe)
            .thenApply(mapIndexData);
    }
}
