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

package org.elasticsearch.plugin.analysis.icu;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.analysis.IcuAnalysisBinderProcessor;
import org.elasticsearch.indices.analysis.IcuIndicesAnalysisModule;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class AnalysisICUPlugin extends Plugin {

    @Override
    public String name() {
        return "analysis-icu";
    }

    @Override
    public String description() {
        return "UTF related ICU analysis support";
    }

    @Override
    public Collection<Module> nodeModules() {
        return Collections.<Module>singletonList(new IcuIndicesAnalysisModule());
    }

    /**
     * Automatically called with the analysis module.
     */
    public void onModule(AnalysisModule module) {
        module.addProcessor(new IcuAnalysisBinderProcessor());
    }
}
