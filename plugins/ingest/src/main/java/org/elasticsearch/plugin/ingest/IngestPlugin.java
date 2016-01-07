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

package org.elasticsearch.plugin.ingest;

import org.elasticsearch.ingest.IngestModule;
import org.elasticsearch.ingest.processor.GeoIpProcessor;
import org.elasticsearch.ingest.processor.GrokProcessor;
import org.elasticsearch.plugins.Plugin;

public class IngestPlugin extends Plugin {

    public static final String NAME = "ingest";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Plugin that allows to plug in ingest processors";
    }

    public void onModule(IngestModule ingestModule) {
        ingestModule.registerProcessor(GeoIpProcessor.TYPE, (environment, templateService) -> new GeoIpProcessor.Factory(environment.configFile()));
        ingestModule.registerProcessor(GrokProcessor.TYPE, (environment, templateService) -> new GrokProcessor.Factory(environment.configFile()));
    }
}
