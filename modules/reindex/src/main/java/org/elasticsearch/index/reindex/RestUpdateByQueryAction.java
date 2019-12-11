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
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.script.Script.DEFAULT_SCRIPT_LANG;

public class RestUpdateByQueryAction extends AbstractBulkByQueryRestHandler<UpdateByQueryRequest, UpdateByQueryAction> {
    public RestUpdateByQueryAction(RestController controller) {
        super(UpdateByQueryAction.INSTANCE);
        controller.registerHandler(POST, "/{index}/_update_by_query", this);
    }

    @Override
    public String getName() {
        return "update_by_query_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return doPrepareRequest(request, client, false, true);
    }

    @Override
    protected UpdateByQueryRequest buildRequest(RestRequest request) throws IOException {
        /*
         * Passing the search request through UpdateByQueryRequest first allows
         * it to set its own defaults which differ from SearchRequest's
         * defaults. Then the parse can override them.
         */
        UpdateByQueryRequest internal = new UpdateByQueryRequest();

        Map<String, Consumer<Object>> consumers = new HashMap<>();
        consumers.put("conflicts", o -> internal.setConflicts((String) o));
        consumers.put("script", o -> internal.setScript(parseScript(o)));
        consumers.put("max_docs", s -> setMaxDocsValidateIdentical(internal, ((Number) s).intValue()));

        parseInternalRequest(internal, request, consumers);

        internal.setPipeline(request.param("pipeline"));
        return internal;
    }

    @SuppressWarnings("unchecked")
    private static Script parseScript(Object config) {
        assert config != null : "Script should not be null";

        if (config instanceof String) {
            return new Script((String) config);
        } else if (config instanceof Map) {
            Map<String,Object> configMap = (Map<String, Object>) config;
            String script = null;
            ScriptType type = null;
            String lang = null;
            Map<String, Object> params = Collections.emptyMap();
            for (Iterator<Map.Entry<String, Object>> itr = configMap.entrySet().iterator(); itr.hasNext();) {
                Map.Entry<String, Object> entry = itr.next();
                String parameterName = entry.getKey();
                Object parameterValue = entry.getValue();
                if (Script.LANG_PARSE_FIELD.match(parameterName, LoggingDeprecationHandler.INSTANCE)) {
                    if (parameterValue instanceof String || parameterValue == null) {
                        lang = (String) parameterValue;
                    } else {
                        throw new ElasticsearchParseException("Value must be of type String: [" + parameterName + "]");
                    }
                } else if (Script.PARAMS_PARSE_FIELD.match(parameterName, LoggingDeprecationHandler.INSTANCE)) {
                    if (parameterValue instanceof Map || parameterValue == null) {
                        params = (Map<String, Object>) parameterValue;
                    } else {
                        throw new ElasticsearchParseException("Value must be of type String: [" + parameterName + "]");
                    }
                } else if (ScriptType.INLINE.getParseField().match(parameterName, LoggingDeprecationHandler.INSTANCE)) {
                    if (parameterValue instanceof String || parameterValue == null) {
                        script = (String) parameterValue;
                        type = ScriptType.INLINE;
                    } else {
                        throw new ElasticsearchParseException("Value must be of type String: [" + parameterName + "]");
                    }
                } else if (ScriptType.STORED.getParseField().match(parameterName, LoggingDeprecationHandler.INSTANCE)) {
                    if (parameterValue instanceof String || parameterValue == null) {
                        script = (String) parameterValue;
                        type = ScriptType.STORED;
                    } else {
                        throw new ElasticsearchParseException("Value must be of type String: [" + parameterName + "]");
                    }
                }
            }
            if (script == null) {
                throw new ElasticsearchParseException("expected one of [{}] or [{}] fields, but found none",
                    ScriptType.INLINE.getParseField().getPreferredName(), ScriptType.STORED.getParseField().getPreferredName());
            }
            assert type != null : "if script is not null, type should definitely not be null";

            if (type == ScriptType.STORED) {
                if (lang != null) {
                    throw new IllegalArgumentException("lang cannot be specified for stored scripts");
                }

                return new Script(type, null, script, null, params);
            } else {
                return new Script(type, lang == null ? DEFAULT_SCRIPT_LANG : lang, script, params);
            }
        } else {
            throw new IllegalArgumentException("Script value should be a String or a Map");
        }
    }
}
