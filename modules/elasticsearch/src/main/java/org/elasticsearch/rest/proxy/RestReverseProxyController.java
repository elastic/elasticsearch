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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;

import java.util.Map;

/**
  * This class stores instances of reverse proxies and a map between rest actions and their rest proxies
  * @author itaifrenkel
 */
public class RestReverseProxyController extends AbstractLifecycleComponent<RestReverseProxyController> {

    private final Map<String, RestReverseProxy> reverseProxyImpl;
    private final RestReverseProxyMap proxyClasses;

    @Inject public RestReverseProxyController(Settings settings,  RestReverseProxyMap proxiesClasses) {
        super(settings);
        this.reverseProxyImpl = Maps.newHashMap();
        this.proxyClasses =proxiesClasses;
    }

    @Override protected void doStart() throws ElasticSearchException {
    }

    @Override protected void doStop() throws ElasticSearchException {
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    public void addProxy(RestReverseProxy proxy) {
        Class<? extends RestReverseProxy> clazz = proxy.getClass();
        reverseProxyImpl.put(clazz.getName(), proxy);
    }

    public RestHandler proxyRestAction(RestHandler restAction) {

        Class<? extends RestReverseProxy> proxyClass = proxyClasses.get(restAction.getClass());

        if (proxyClass != null) {
            // specified rest action has a registered rest proxy
            RestReverseProxy proxy = reverseProxyImpl.get(proxyClass.getName());
            if (proxy == null) {
                throw new IllegalStateException("No instance of proxy class " + proxyClass + " was added to " + this.getClass());
            }
            restAction = createRestHandler(proxy ,restAction);
        }
        return restAction;
    }

    private RestHandler createRestHandler(final RestReverseProxy proxy, final RestHandler restAction) {
        return
            new RestHandler() {
                        @Override public void handleRequest(RestRequest request, RestChannel channel) {
                            proxy.handleRequest(request,channel,restAction);
                        }
                    };
    }

}
