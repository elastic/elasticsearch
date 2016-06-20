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

package org.elasticsearch.node;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;

/**
 * A node for testing which allows:
 * <ul>
 *   <li>Overriding Version.CURRENT</li>
 *   <li>Adding test plugins that exist on the classpath</li>
 * </ul>
 */
public class MockNode extends Node {

    private final boolean mockBigArrays;
    private Collection<Class<? extends Plugin>> plugins;

    public MockNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
        super(InternalSettingsPreparer.prepareEnvironment(settings, null), classpathPlugins);
        this.plugins = classpathPlugins;
        this.mockBigArrays = classpathPlugins.contains(NodeMocksPlugin.class); // if this plugin is present we mock bigarrays :)
    }

    public Collection<Class<? extends Plugin>> getPlugins() {
        return plugins;
    }

    @Override
    protected BigArrays createNewBigArrays(Settings settings, CircuitBreakerService circuitBreakerService) {
        if (mockBigArrays) {
            return new MockBigArrays(settings, circuitBreakerService);
        } else {
            return super.createNewBigArrays(settings, circuitBreakerService);
        }
    }
}
