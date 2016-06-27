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

import org.elasticsearch.action.search.template.MultiSearchTemplateAction;
import org.elasticsearch.action.search.template.SearchTemplateAction;
import org.elasticsearch.action.search.template.TransportMultiSearchTemplateAction;
import org.elasticsearch.action.search.template.TransportSearchTemplateAction;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.action.search.template.RestDeleteSearchTemplateAction;
import org.elasticsearch.rest.action.search.template.RestGetSearchTemplateAction;
import org.elasticsearch.rest.action.search.template.RestMultiSearchTemplateAction;
import org.elasticsearch.rest.action.search.template.RestPutSearchTemplateAction;
import org.elasticsearch.rest.action.search.template.RestRenderSearchTemplateAction;
import org.elasticsearch.rest.action.search.template.RestSearchTemplateAction;
import org.elasticsearch.script.ScriptEngineService;

import java.util.Arrays;
import java.util.List;

public class MustachePlugin extends Plugin implements ScriptPlugin, ActionPlugin {

    @Override
    public ScriptEngineService getScriptEngineService(Settings settings) {
        return new MustacheScriptEngineService(settings);
    }

    @Override
    public List<ActionHandler<?, ?>> getActions() {
        return Arrays.asList(new ActionHandler<>(SearchTemplateAction.INSTANCE, TransportSearchTemplateAction.class),
                new ActionHandler<>(MultiSearchTemplateAction.INSTANCE, TransportMultiSearchTemplateAction.class));
    }

    public void onModule(NetworkModule module) {
        if (module.isTransportClient() == false) {
            module.registerRestHandler(RestSearchTemplateAction.class);
            module.registerRestHandler(RestMultiSearchTemplateAction.class);
            module.registerRestHandler(RestGetSearchTemplateAction.class);
            module.registerRestHandler(RestPutSearchTemplateAction.class);
            module.registerRestHandler(RestDeleteSearchTemplateAction.class);
            module.registerRestHandler(RestRenderSearchTemplateAction.class);
        }
    }
}
