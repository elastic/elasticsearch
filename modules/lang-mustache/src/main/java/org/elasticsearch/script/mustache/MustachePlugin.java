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

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.template.MultiSearchTemplateAction;
import org.elasticsearch.action.search.template.SearchTemplateAction;
import org.elasticsearch.action.search.template.TransportMultiSearchTemplateAction;
import org.elasticsearch.action.search.template.TransportSearchTemplateAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.action.search.template.RestDeleteSearchTemplateAction;
import org.elasticsearch.rest.action.search.template.RestGetSearchTemplateAction;
import org.elasticsearch.rest.action.search.template.RestMultiSearchTemplateAction;
import org.elasticsearch.rest.action.search.template.RestPutSearchTemplateAction;
import org.elasticsearch.rest.action.search.template.RestRenderSearchTemplateAction;
import org.elasticsearch.rest.action.search.template.RestSearchTemplateAction;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.search.SearchModule;

import java.util.Arrays;
import java.util.List;

public class MustachePlugin extends Plugin implements ScriptPlugin, ActionPlugin {

    @Override
    public ScriptEngineService getScriptEngineService(Settings settings) {
        return new MustacheScriptEngineService(settings);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(SearchTemplateAction.INSTANCE, TransportSearchTemplateAction.class),
                new ActionHandler<>(MultiSearchTemplateAction.INSTANCE, TransportMultiSearchTemplateAction.class));
    }

    public void onModule(SearchModule module) {
        module.registerQuery(TemplateQueryBuilder::new, TemplateQueryBuilder::fromXContent, TemplateQueryBuilder.QUERY_NAME_FIELD);
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        return Arrays.asList(RestSearchTemplateAction.class, RestMultiSearchTemplateAction.class, RestGetSearchTemplateAction.class,
                RestPutSearchTemplateAction.class, RestDeleteSearchTemplateAction.class, RestRenderSearchTemplateAction.class);
    }
}
