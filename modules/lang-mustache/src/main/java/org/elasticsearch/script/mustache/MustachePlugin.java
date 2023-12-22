/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class MustachePlugin extends Plugin implements ScriptPlugin, ActionPlugin, SearchPlugin {

    public static final ActionType<SearchTemplateResponse> SEARCH_TEMPLATE_ACTION = new ActionType<>(
        "indices:data/read/search/template",
        SearchTemplateResponse::new
    );
    public static final ActionType<MultiSearchTemplateResponse> MULTI_SEARCH_TEMPLATE_ACTION = new ActionType<>(
        "indices:data/read/msearch/template",
        MultiSearchTemplateResponse::new
    );

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new MustacheScriptEngine();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(SEARCH_TEMPLATE_ACTION, TransportSearchTemplateAction.class),
            new ActionHandler<>(MULTI_SEARCH_TEMPLATE_ACTION, TransportMultiSearchTemplateAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(RestHandlerParameters parameters) {
        return Arrays.asList(
            new RestSearchTemplateAction(),
            new RestMultiSearchTemplateAction(parameters.settings()),
            new RestRenderSearchTemplateAction()
        );
    }
}
