/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.plugin.rest.hellorest;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.hellorest.action.hello.RestPostHelloAction;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.rest.action.admin.cluster.health.RestClusterHealthAction;
import org.elasticsearch.rest.action.admin.cluster.state.RestClusterStateAction;
import org.elasticsearch.rest.action.get.RestGetAction;
import org.elasticsearch.rest.action.main.RestMainAction;

/**
 *  This is an example plugin that shows how to selectively enable  default elasticsearch REST APIs,
 *  and how to add a custom REST API.
 *
 *  $ curl -X POST "http://localhost:9200/articles/article/_hello"
 * {"ok":true,"_index":"articles","_type":"article","_id":"t_LGwhhHTt2pBFiixUktOA"}
 *
 * $ curl -XGET 'http://localhost:9200/articles/article/t_LGwhhHTt2pBFiixUktOA'
 * {"_index":"articles","_type":"article","_id":"t_LGwhhHTt2pBFiixUktOA","_version":1, "_source" : {"hello":"rest"}}
 *
 *  Install the plugin by copying the plugin JAR file to the plugins/rest-hellorest/ directory .
 *  Disable the plugin with the -Dhellorest.enabled=false commandline argument
 *
 * @author itaifrenkel
 */
public class HelloRestPlugin extends AbstractPlugin {

    private final Settings settings;

    public HelloRestPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override public String name() {
        return "rest-hellorest";
    }

    @Override public String description() {
        return "Extends or replaces elasticsearch REST APIs. Requires rest.enabled=false";
    }

     @Override public void processModule(Module module) {
         if (settings.getAsBoolean("hellorest.enabled", true) &&
               module instanceof RestModule) {

                RestModule restModule = (RestModule) module;

                //enable only specific REST actions
                restModule.removeDefaultRestActions();
                restModule.addRestAction(RestMainAction.class);
                restModule.addRestAction(RestGetAction.class);
                restModule.addRestAction(RestClusterStateAction.class);
                restModule.addRestAction(RestClusterHealthAction.class);

                //add our own custom hello rest  actions
                restModule.addRestAction(RestPostHelloAction.class);
         }
    }
}