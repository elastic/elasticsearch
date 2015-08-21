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

package org.elasticsearch.plugin.deletebyquery;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.rest.action.deletebyquery.RestDeleteByQueryAction;

import java.util.Collection;
import java.util.Collections;

public class DeleteByQueryPlugin extends Plugin {

    public static final String NAME = "delete-by-query";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Elasticsearch Delete-By-Query Plugin";
    }

    public void onModule(ActionModule actionModule) {
        actionModule.registerAction(DeleteByQueryAction.INSTANCE, TransportDeleteByQueryAction.class);
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(RestDeleteByQueryAction.class);
    }

}
