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

package org.elasticsearch.rest.proxy;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestHandler;

/**
 * An elasticsearch  module that is used by plugins to register a new RestReverseProxy class
 *
 * It's configure method binds the RestReverseProxyController and its dependencies
 */
public class RestReverseProxyModule  extends AbstractModule {

    private final RestReverseProxyMap restProxyMap;
    private final Settings settings;

    public RestReverseProxyModule(Settings settings) {
        this.settings = settings;
        restProxyMap = new RestReverseProxyMap();
    }

    public void addDefaultRestReverseProxy(Class<? extends RestReverseProxy> reverseProxy) {
        // a special key placeholder for the default rest reverse proxy
        restProxyMap.setDefault(reverseProxy);
    }

    public void addRestReverseProxy(Class<? extends RestReverseProxy> reverseProxy, Class<? extends RestHandler> restHandlerClass) {
        restProxyMap.put(restHandlerClass, reverseProxy);
    }

    @Override protected void configure() {

      // create new rest proxy controller and inject which rest handler deserves which rest reverse proxies.
      bind(RestReverseProxyMap.class).toInstance(restProxyMap);
      bind(RestReverseProxyController.class).asEagerSingleton();

     // create new rest proxies. Each proxy constructor must register with the controller
    for (Class<? extends RestReverseProxy> proxyClass : restProxyMap.proxies()) {
        bind(proxyClass).asEagerSingleton();
    }
    }
}
