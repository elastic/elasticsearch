/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class RestCreateIndexActionV7 extends RestCreateIndexAction {

    /**
     * Parameter that controls whether certain REST apis should include type names in their requests.
     */
    public static final String INCLUDE_TYPE_NAME_PARAMETER = "include_type_name";
    public static final boolean DEFAULT_INCLUDE_TYPE_NAME_POLICY = false;

    @Override
    public String getName() {
        return super.getName() + "_v7";
    }

    @Override
    public Version compatibleWithVersion() {
        return Version.V_7_0_0;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        CreateIndexRequest createIndexRequest = prepareV7Request(request);
        return channel -> client.admin().indices().create(createIndexRequest, new RestToXContentListener<>(channel));
    }

    // default scope for testing
    CreateIndexRequest prepareV7Request(RestRequest request) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(request.param("index"));

        if (request.hasContent()) {
            Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false, request.getXContentType()).v2();

            request.param(INCLUDE_TYPE_NAME_PARAMETER);// just consume, it is always replaced with _doc
            sourceAsMap = prepareMappingsV7(sourceAsMap, request);

            createIndexRequest.source(sourceAsMap, LoggingDeprecationHandler.INSTANCE);
        }

        createIndexRequest.timeout(request.paramAsTime("timeout", createIndexRequest.timeout()));
        createIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", createIndexRequest.masterNodeTimeout()));
        createIndexRequest.waitForActiveShards(ActiveShardCount.parseString(request.param("wait_for_active_shards")));
        return createIndexRequest;
    }

    static Map<String, Object> prepareMappingsV7(Map<String, Object> source, RestRequest request) {
        final boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, false);

        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) source.get("mappings");

        if (includeTypeName && mappings.size() == 1) {
            Map<String, Object> newSource = new HashMap<>();

            String typeName = mappings.keySet().iterator().next();
            @SuppressWarnings("unchecked")
            Map<String, Object> typedMappings = (Map<String, Object>) mappings.get(typeName);

            // no matter what the type was, replace it with _doc, because the internal representation still uses single type `_doc`.
            newSource.put("mappings", Collections.singletonMap(MapperService.SINGLE_MAPPING_NAME, typedMappings));
            return newSource;
        } else {
            return prepareMappings(source);
        }
    }

    static Map<String, Object> prepareMappings(Map<String, Object> source) {
        if (source.containsKey("mappings") == false || (source.get("mappings") instanceof Map) == false) {
            return source;
        }

        Map<String, Object> newSource = new HashMap<>(source);

        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) source.get("mappings");
        if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, mappings)) {
            throw new IllegalArgumentException("The mapping definition cannot be nested under a type");
        }

        newSource.put("mappings", singletonMap(MapperService.SINGLE_MAPPING_NAME, mappings));
        return newSource;
    }
}
