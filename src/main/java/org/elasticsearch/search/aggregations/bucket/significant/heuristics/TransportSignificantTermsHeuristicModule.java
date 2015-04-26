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


package org.elasticsearch.search.aggregations.bucket.significant.heuristics;

import com.google.common.collect.Lists;

import org.elasticsearch.common.inject.AbstractModule;

import java.util.List;


public class TransportSignificantTermsHeuristicModule extends AbstractModule {

    private List<SignificanceHeuristicStreams.Stream> streams = Lists.newArrayList();

    public TransportSignificantTermsHeuristicModule() {
        registerStream(JLHScore.STREAM);
        registerStream(PercentageScore.STREAM);
        registerStream(MutualInformation.STREAM);
        registerStream(GND.STREAM);
        registerStream(ChiSquare.STREAM);
        registerStream(ScriptHeuristic.STREAM);
    }

    public void registerStream(SignificanceHeuristicStreams.Stream stream) {
        streams.add(stream);
    }

    @Override
    protected void configure() {
        for (SignificanceHeuristicStreams.Stream stream : streams) {
            SignificanceHeuristicStreams.registerStream(stream, stream.getName());
        }
    }
}
