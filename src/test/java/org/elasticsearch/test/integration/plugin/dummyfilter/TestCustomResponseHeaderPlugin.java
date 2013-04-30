/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test.integration.nodesinfo.plugin.dummyfilter;

import static com.google.common.collect.Lists.newArrayList;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Collection;

public class TestCustomResponseHeaderPlugin extends AbstractPlugin {

    static final public class Fields {
        static public final String NAME = "test-plugin-custom-header";
        static public final String DESCRIPTION = NAME + " description";
    }

    private final Settings settings;

    @Inject public TestCustomResponseHeaderPlugin(Settings settings) {
        this.settings = settings;
    }
    @Override
    public String name() {
        return Fields.NAME;
    }

    @Override
    public String description() {
        return Fields.DESCRIPTION;
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        modules.add(TestHttpServerModule.class);
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = newArrayList();
        services.add(TestHttpServer.class);
        return services;
    }

    @Override
    public Settings additionalSettings() {
        return ImmutableSettings.settingsBuilder().put("http.enabled", false).build();
    }

}
