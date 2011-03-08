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

package org.elasticsearch.index.mapper.xcontent;

import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettingsModule;

/**
 * @author kimchy (shay.banon)
 */
public class MapperTests {

    public static XContentDocumentMapperParser newParser() {
        return new XContentDocumentMapperParser(new Index("test"), newAnalysisService());
    }

    public static MapperService newMapperService() {
        return new MapperService(new Index("test"), ImmutableSettings.Builder.EMPTY_SETTINGS, new Environment(), newAnalysisService());
    }

    public static AnalysisService newAnalysisService() {
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(new Index("test"), ImmutableSettings.Builder.EMPTY_SETTINGS),
                new IndexNameModule(new Index("test")),
                new AnalysisModule(ImmutableSettings.Builder.EMPTY_SETTINGS)).createInjector();

        return injector.getInstance(AnalysisService.class);
    }
}
