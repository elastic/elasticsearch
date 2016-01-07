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
import org.elasticsearch.ingest.processor.AppendProcessor;
import org.elasticsearch.ingest.processor.ConvertProcessor;
import org.elasticsearch.ingest.processor.DateProcessor;
import org.elasticsearch.ingest.processor.FailProcessor;
import org.elasticsearch.ingest.processor.GeoIpProcessor;
import org.elasticsearch.ingest.processor.GrokProcessor;
import org.elasticsearch.ingest.processor.GsubProcessor;
import org.elasticsearch.ingest.processor.JoinProcessor;
import org.elasticsearch.ingest.processor.LowercaseProcessor;
import org.elasticsearch.ingest.processor.RemoveProcessor;
import org.elasticsearch.ingest.processor.RenameProcessor;
import org.elasticsearch.ingest.processor.SetProcessor;
import org.elasticsearch.ingest.processor.SplitProcessor;
import org.elasticsearch.ingest.processor.TrimProcessor;
import org.elasticsearch.ingest.processor.UppercaseProcessor;
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
        ingestModule.registerProcessor(DateProcessor.TYPE, (environment, templateService) -> new DateProcessor.Factory());
        ingestModule.registerProcessor(SetProcessor.TYPE, (environment, templateService) -> new SetProcessor.Factory(templateService));
        ingestModule.registerProcessor(AppendProcessor.TYPE, (environment, templateService) -> new AppendProcessor.Factory(templateService));
        ingestModule.registerProcessor(RenameProcessor.TYPE, (environment, templateService) -> new RenameProcessor.Factory());
        ingestModule.registerProcessor(RemoveProcessor.TYPE, (environment, templateService) -> new RemoveProcessor.Factory(templateService));
        ingestModule.registerProcessor(SplitProcessor.TYPE, (environment, templateService) -> new SplitProcessor.Factory());
        ingestModule.registerProcessor(JoinProcessor.TYPE, (environment, templateService) -> new JoinProcessor.Factory());
        ingestModule.registerProcessor(UppercaseProcessor.TYPE, (environment, templateService) -> new UppercaseProcessor.Factory());
        ingestModule.registerProcessor(LowercaseProcessor.TYPE, (environment, templateService) -> new LowercaseProcessor.Factory());
        ingestModule.registerProcessor(TrimProcessor.TYPE, (environment, templateService) -> new TrimProcessor.Factory());
        ingestModule.registerProcessor(ConvertProcessor.TYPE, (environment, templateService) -> new ConvertProcessor.Factory());
        ingestModule.registerProcessor(GsubProcessor.TYPE, (environment, templateService) -> new GsubProcessor.Factory());
        ingestModule.registerProcessor(FailProcessor.TYPE, (environment, templateService) -> new FailProcessor.Factory(templateService));
    }
}
