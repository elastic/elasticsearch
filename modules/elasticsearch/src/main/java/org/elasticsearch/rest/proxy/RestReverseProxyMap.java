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

import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.rest.RestHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RestReverseProxyMap {

    private Class<? extends RestReverseProxy> defaultRestProxyClass;
    private final Map<Class<? extends RestHandler>, Class<? extends RestReverseProxy>> restProxyClass;

   public  RestReverseProxyMap() {
        this.restProxyClass = new HashMap<Class<? extends RestHandler>, Class<? extends RestReverseProxy>>();
        this.defaultRestProxyClass = null;
     }

    public void setDefault(Class<? extends RestReverseProxy> reverseProxies) {
        if (reverseProxies == null) {
            throw new IllegalArgumentException("reverseProxies cannot be null");
        }
        this.defaultRestProxyClass = reverseProxies;
    }

    public void put(Class<? extends RestHandler> restHandlerClass, Class<? extends RestReverseProxy> reverseProxy) {
        if (reverseProxy == null) {
            // null means revert to default
            throw new IllegalArgumentException("reverseProxy cannot be null");
        }
        restProxyClass.put(restHandlerClass, reverseProxy);
    }

    public Class<? extends RestReverseProxy> get(Class<? extends RestHandler> restHandlerClass) {
        Class<? extends RestReverseProxy> proxy = restProxyClass.get(restHandlerClass);
        if (proxy == null) {
            proxy = defaultRestProxyClass;
        }
        return proxy;
    }

    public Iterable<? extends Class<? extends RestReverseProxy>> proxies() {
        Set<Class<? extends RestReverseProxy>> proxies = Sets.newHashSet();
        proxies.addAll(restProxyClass.values());
        if (defaultRestProxyClass != null) {
            proxies.add(defaultRestProxyClass);
        }
        return proxies;
    }
}
