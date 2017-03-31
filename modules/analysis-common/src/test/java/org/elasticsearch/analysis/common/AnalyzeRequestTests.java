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

package org.elasticsearch.analysis.common;

import org.elasticsearch.action.IndicesRequestTestCase;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Tests routing the analyze request.
 */
public class AnalyzeRequestTests extends IndicesRequestTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.addAll(super.nodePlugins());
        plugins.add(CommonAnalysisPlugin.class);
        return plugins;
    }

    public void testAnalyze() {
        String analyzeShardAction = AnalyzeAction.NAME + "[s]";
        interceptTransportActions(analyzeShardAction);

        AnalyzeRequest analyzeRequest = new AnalyzeRequest(randomIndexOrAlias());
        analyzeRequest.text("text");
        internalCluster().coordOnlyNodeClient().admin().indices()
                .analyze(analyzeRequest).actionGet();

        clearInterceptedActions();
        assertSameIndices(analyzeRequest, analyzeShardAction);
    }
}
