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

import static org.elasticsearch.graphql.api.GqlApiUtils.getJavaUtilBuilderResult;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.javautil.JavaUtilXContentGenerator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GqlMockApi implements GqlApi {
    /*
    @Override
    public Map<String, Object> getHello() {
        LinkedHashMap<String, Object> result = new LinkedHashMap();
        result.put("name", "some-name");
        result.put("cluster_name", "elasticsearch");
        result.put("cluster_uuid", "1-D8qMuTS6qhoRIIUIq4Vw");

        LinkedHashMap<String, Object> version = new LinkedHashMap();
        version.put("number", "8.0.0-SNAPSHOT");
        version.put("build_flavor", "default");
        version.put("build_type", "tar");
        version.put("build_hash", "2aebbe6");
        version.put("build_date", "2019-07-21T16:10:22.420795Z");
        version.put("build_snapshot", true);
        version.put("lucene_version", "8.2.0");
        version.put("minimum_wire_compatibility_version", "7.4.0");
        version.put("minimum_index_compatibility_version", "7.0.0");
        result.put("version", version);

        result.put("tagline", "You Know, for Search");

        return result;
    }
    */

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Map<String, Object>> getInfo() throws Exception {
        XContentBuilder builder = GqlApiUtils.createJavaUtilBuilder();
        builder
            .startObject()
                .field("name", "some-name")
                .field("cluster_name", "elasticsearch")
                .field("cluster_uuid", "1-D8qMuTS6qhoRIIUIq4Vw")
                .startObject("version")
                    .field("number", "8.0.0-SNAPSHOT")
                    .field("build_flavor", "default")
                    .field("build_type", "tar")
                    .field("build_hash", "2aebbe6")
                    .field("build_date", "2019-07-21T16:10:22.420795Z")
                    .field("build_snapshot", true)
                    .field("lucene_version", "8.2.0")
                    .field("minimum_wire_compatibility_version", "7.4.0")
                    .field("minimum_index_compatibility_version", "7.0.0")
                .endObject()
                .field("tagline", "You Know, for Search")
            .endObject();


        CompletableFuture<Map<String, Object>> promise = new CompletableFuture<>();
        promise.complete((Map) ((JavaUtilXContentGenerator) builder.generator()).getResult());
        return promise;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<List<Object>> getIndexInfos() throws Exception {
        XContentBuilder builder = GqlApiUtils.createJavaUtilBuilder();
        builder
            .startArray()
                .startObject()
                    .field("health", "green")
                    .field("status", "open")
                    .field("index", ".security-7")
                    .field("uuid", "Osf5_rsiQzmJnjvXsYz5HQ")
                    .field("pri", "1")
                    .field("rep", "0")
                    .field("docs.count", "6")
                    .field("docs.deleted", "0")
                    .field("store.size", "19.7Kb")
                    .field("pri.store.size", "19.7Kb")
                .endObject()
            .endArray();

        return CompletableFuture.completedFuture((List) getJavaUtilBuilderResult(builder));
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Map<String, Object>> getIndex(String indexName) throws Exception {
        XContentBuilder builder = GqlApiUtils.createJavaUtilBuilder();
        builder
            .startArray()
            .endArray();

        return CompletableFuture.completedFuture((Map) getJavaUtilBuilderResult(builder));
    }

    @Override
    public CompletableFuture<Map<String, Object>> getDocument(String indexName, String documentId) throws Exception {
        return CompletableFuture.completedFuture(new HashMap<>());
    }
}
