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

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.script.Script.ScriptField;

public class RestUpdateByQueryAction extends AbstractBulkByQueryRestHandler<UpdateByQueryRequest, UpdateByQueryAction> {

    @Inject
    public RestUpdateByQueryAction(Settings settings, RestController controller,
            SearchRequestParsers searchRequestParsers, ClusterService clusterService) {
        super(settings, searchRequestParsers, clusterService, UpdateByQueryAction.INSTANCE);
        controller.registerHandler(POST, "/{index}/_update_by_query", this);
        controller.registerHandler(POST, "/{index}/{type}/_update_by_query", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        handleRequest(request, channel, client, false, true);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected UpdateByQueryRequest buildRequest(RestRequest request) throws IOException {
        /*
         * Passing the search request through UpdateByQueryRequest first allows
         * it to set its own defaults which differ from SearchRequest's
         * defaults. Then the parse can override them.
         */
        UpdateByQueryRequest internal = new UpdateByQueryRequest(new SearchRequest());

        Map<String, Consumer<Object>> consumers = new HashMap<>();
        consumers.put("conflicts", o -> internal.setConflicts((String) o));
        consumers.put("script", o -> internal.setScript(parseScript((Map<String, Object>)o, parseFieldMatcher)));

        parseInternalRequest(internal, request, consumers);

        internal.setPipeline(request.param("pipeline"));
        return internal;
    }

    @SuppressWarnings("unchecked")
    static Script parseScript(Map<String, Object> config, ParseFieldMatcher parseFieldMatcher) {
        String script = null;
        ScriptService.ScriptType type = null;
        String lang = null;
        Map<String, Object> params = null;
        for (Iterator<Map.Entry<String, Object>> itr = config.entrySet().iterator(); itr.hasNext();) {
            Map.Entry<String, Object> entry = itr.next();
            String parameterName = entry.getKey();
            Object parameterValue = entry.getValue();
            if (parseFieldMatcher.match(parameterName, ScriptField.LANG)) {
                if (parameterValue instanceof String || parameterValue == null) {
                    lang = (String) parameterValue;
                } else {
                    throw new ElasticsearchParseException("Value must be of type String: [" + parameterName + "]");
                }
            } else if (parseFieldMatcher.match(parameterName, ScriptField.PARAMS)) {
                if (parameterValue instanceof Map || parameterValue == null) {
                    params = (Map<String, Object>) parameterValue;
                } else {
                    throw new ElasticsearchParseException("Value must be of type String: [" + parameterName + "]");
                }
            } else if (parseFieldMatcher.match(parameterName, ScriptService.ScriptType.INLINE.getParseField())) {
                if (parameterValue instanceof String || parameterValue == null) {
                    script = (String) parameterValue;
                    type = ScriptService.ScriptType.INLINE;
                } else {
                    throw new ElasticsearchParseException("Value must be of type String: [" + parameterName + "]");
                }
            } else if (parseFieldMatcher.match(parameterName, ScriptService.ScriptType.FILE.getParseField())) {
                if (parameterValue instanceof String || parameterValue == null) {
                    script = (String) parameterValue;
                    type = ScriptService.ScriptType.FILE;
                } else {
                    throw new ElasticsearchParseException("Value must be of type String: [" + parameterName + "]");
                }
            } else if (parseFieldMatcher.match(parameterName, ScriptService.ScriptType.STORED.getParseField())) {
                if (parameterValue instanceof String || parameterValue == null) {
                    script = (String) parameterValue;
                    type = ScriptService.ScriptType.STORED;
                } else {
                    throw new ElasticsearchParseException("Value must be of type String: [" + parameterName + "]");
                }
            }
        }
        if (script == null) {
            throw new ElasticsearchParseException("expected one of [{}], [{}] or [{}] fields, but found none",
                    ScriptService.ScriptType.INLINE.getParseField().getPreferredName(), ScriptService.ScriptType.FILE.getParseField()
                    .getPreferredName(), ScriptService.ScriptType.STORED.getParseField().getPreferredName());
        }
        assert type != null : "if script is not null, type should definitely not be null";
        return new Script(script, type, lang, params);
    }
}
