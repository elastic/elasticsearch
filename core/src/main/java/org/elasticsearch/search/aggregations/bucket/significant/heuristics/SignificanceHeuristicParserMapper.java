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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.script.ScriptService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SignificanceHeuristicParserMapper {

    protected final Map<String, SignificanceHeuristicParser> significanceHeuristicParsers;

    @Inject
    public SignificanceHeuristicParserMapper(Set<SignificanceHeuristicParser> parsers, ScriptService scriptService) {
        Map<String, SignificanceHeuristicParser> map = new HashMap<>();
        add(map, new JLHScore.JLHScoreParser());
        add(map, new PercentageScore.PercentageScoreParser());
        add(map, new MutualInformation.MutualInformationParser());
        add(map, new ChiSquare.ChiSquareParser());
        add(map, new GND.GNDParser());
        add(map, new ScriptHeuristic.ScriptHeuristicParser(scriptService));
        for (SignificanceHeuristicParser parser : parsers) {
            add(map, parser);
        }
        significanceHeuristicParsers = Collections.unmodifiableMap(map);
    }

    public SignificanceHeuristicParser get(String parserName) {
        return significanceHeuristicParsers.get(parserName);
    }


    private void add(Map<String, SignificanceHeuristicParser> map, SignificanceHeuristicParser parser) {
        for (String type : parser.getNames()) {
            map.put(type, parser);
        }
    }
}
