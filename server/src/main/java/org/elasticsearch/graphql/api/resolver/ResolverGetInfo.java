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

import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.client.node.NodeClient;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.elasticsearch.graphql.api.GqlApiUtils.executeAction;

public class ResolverGetInfo {

    @SuppressWarnings("unchecked")
    public static CompletableFuture<Map<String, Object>> exec(NodeClient client) throws Exception {
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
}
