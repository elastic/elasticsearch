/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.script.ScriptType;

import java.util.Map;

public class SearchTemplateRequestBuilder extends ActionRequestBuilder<SearchTemplateRequest, SearchTemplateResponse> {

    public SearchTemplateRequestBuilder(ElasticsearchClient client) {
        super(client, SearchTemplateAction.INSTANCE, new SearchTemplateRequest());
    }

    public SearchTemplateRequestBuilder setRequest(SearchRequest searchRequest) {
        request.setRequest(searchRequest);
        return this;
    }

    public SearchTemplateRequestBuilder setSimulate(boolean simulate) {
        request.setSimulate(simulate);
        return this;
    }

    /**
     * Enables explanation for each hit on how its score was computed. Disabled by default
     */
    public SearchTemplateRequestBuilder setExplain(boolean explain) {
        request.setExplain(explain);
        return this;
    }

    /**
     * Enables profiling of the query. Disabled by default
     */
    public SearchTemplateRequestBuilder setProfile(boolean profile) {
        request.setProfile(profile);
        return this;
    }

    public SearchTemplateRequestBuilder setScriptType(ScriptType scriptType) {
        request.setScriptType(scriptType);
        return this;
    }

    public SearchTemplateRequestBuilder setScript(String script) {
        request.setScript(script);
        return this;
    }

    public SearchTemplateRequestBuilder setScriptParams(Map<String, Object> scriptParams) {
        request.setScriptParams(scriptParams);
        return this;
    }
}
