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

package org.elasticsearch.index.engine;

import com.google.inject.AbstractModule;
import org.elasticsearch.index.engine.robin.RobinEngineModule;
import org.elasticsearch.util.guice.ModulesFactory;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class EngineModule extends AbstractModule {

    public static final class EngineSettings {
        public static final String ENGINE_TYPE = "index.engine.type";
    }

    private final Settings settings;

    public EngineModule(Settings settings) {
        this.settings = settings;
    }

    @Override protected void configure() {
        ModulesFactory.createModule(settings.getAsClass(EngineSettings.ENGINE_TYPE, RobinEngineModule.class, "org.elasticsearch.index.engine.", "EngineModule"), settings).configure(binder());
    }
}
