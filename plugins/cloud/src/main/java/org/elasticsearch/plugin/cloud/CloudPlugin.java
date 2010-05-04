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

package org.elasticsearch.plugin.cloud;

import org.elasticsearch.cloud.CloudModule;
import org.elasticsearch.cloud.blobstore.CloudBlobStoreService;
import org.elasticsearch.cloud.compute.CloudComputeService;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.util.component.LifecycleComponent;
import org.elasticsearch.util.guice.inject.Module;
import org.elasticsearch.util.settings.Settings;

import java.util.Collection;

import static org.elasticsearch.util.collect.Lists.*;

/**
 * @author kimchy (shay.banon)
 */
public class CloudPlugin extends AbstractPlugin {

    private final Settings settings;

    public CloudPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override public String name() {
        return "cloud";
    }

    @Override public String description() {
        return "Cloud plugin";
    }

    @Override public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        if (settings.getAsBoolean("cloud.enabled", true)) {
            modules.add(CloudModule.class);
        }
        return modules;
    }

    @Override public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = newArrayList();
        if (settings.getAsBoolean("cloud.enabled", true)) {
            services.add(CloudComputeService.class);
            services.add(CloudBlobStoreService.class);
        }
        return services;
    }
}
