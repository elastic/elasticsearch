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
import org.elasticsearch.hellosecurerest.proxy.DoNothingProxy;
import org.elasticsearch.hellosecurerest.proxy.RequireHttpsTerminationProxy;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.proxy.RestReverseProxyModule;
import org.elasticsearch.rest.action.main.RestMainAction;

/**
 *  This is an example plugin that shows how to add filters (in-process reverse proxies) that handle the request before forwarding them to the registered rest handlers.
 *  The plugin looks for a header indicating that the request has been forwarded from an https reverse proxy, otherwise returns 403 (Forbidden)
 *
 * $ curl --header "X-Forwarded-Proto:HTTPS" -XPUT http://localhost:9200/twitter/user/kimchy -d '{"name" : "Shay Banon" }'
 * {"ok":true,"_index":"twitter","_type":"user","_id":"kimchy","_version":1}
 *
 * $ curl -v -XPUT http://localhost:9200/twitter/user/kimchy -d '{ "name" : "Shay Banon"}'
* HTTP/1.1 403 Forbidden
 *
 *  Install the plugin by copying the plugin JAR file to the plugins/rest-hellosecurerest/ directory .
 *  Disable the plugin with the -Dhellosecurerest.enabled=false commandline argument
 *
 * @author itaifrenkel
 */
public class HelloSecureRestPlugin extends AbstractPlugin {

    private final Settings settings;

    public HelloSecureRestPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override public String name() {
        return "rest-hellosecurerest";
    }

    @Override public String description() {
        return "Extends or replaces elasticsearch REST APIs. Requires rest.enabled=false";
    }

     @Override public void processModule(Module module) {
         if (settings.getAsBoolean("hellosecurerest.enabled", true) &&
               module instanceof RestReverseProxyModule) {

                RestReverseProxyModule restModule = (RestReverseProxyModule) module;

                //require https termination for all Rest actions except the main action
                restModule.addDefaultRestReverseProxy(RequireHttpsTerminationProxy.class);
                restModule.addRestReverseProxy(DoNothingProxy.class,RestMainAction.class);
         }
    }
}