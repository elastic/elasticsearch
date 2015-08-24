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

package org.elasticsearch.plugin.discovery.multicast;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.plugin.discovery.multicast.MulticastZenPing;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;

public class MulticastDiscoveryPlugin extends Plugin {

    private final Settings settings;

    public MulticastDiscoveryPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "discovery-multicast";
    }

    @Override
    public String description() {
        return "Multicast Discovery Plugin";
    }
    
    public void onModule(DiscoveryModule module) {
        if (settings.getAsBoolean("discovery.zen.ping.multicast.enabled", false)) {
            module.addZenPing(MulticastZenPing.class);
        }
    }
}
