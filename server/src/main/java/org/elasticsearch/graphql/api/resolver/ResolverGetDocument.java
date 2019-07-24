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

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.graphql.api.GqlApiUtils;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.elasticsearch.graphql.api.GqlApiUtils.*;

public class ResolverGetDocument {

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

    @SuppressWarnings("unchecked")
    public static CompletableFuture<Map<String, Object>> exec(NodeClient client, String indexName, String documentId) throws Exception {
        GetRequest request = new GetRequest(indexName, documentId);
        request.fetchSourceContext(new FetchSourceContext(true));
        CompletableFuture<GetResponse> future = new CompletableFuture<GetResponse>();
        client.get(request, futureToListener(future));

        return future
            .thenApply(GqlApiUtils::toMapSafe)
            .thenApply(mapDocumentData);
    }
}
