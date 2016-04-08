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

package org.elasticsearch.search.aggregations.pipeline.movavg.models;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Contains a map of all concrete model parsers which can be used to build Models
 */
public class MovAvgModelParserMapper {

    protected Map<String, MovAvgModel.AbstractModelParser> movAvgParsers;

    @Inject
    public MovAvgModelParserMapper(Set<MovAvgModel.AbstractModelParser> parsers) {
        Map<String, MovAvgModel.AbstractModelParser> map = new HashMap<>();
        add(map, new SimpleModel.SimpleModelParser());
        add(map, new LinearModel.LinearModelParser());
        add(map, new EwmaModel.SingleExpModelParser());
        add(map, new HoltLinearModel.DoubleExpModelParser());
        add(map, new HoltWintersModel.HoltWintersModelParser());
        for (MovAvgModel.AbstractModelParser parser : parsers) {
            add(map, parser);
        }
        movAvgParsers = Collections.unmodifiableMap(map);
    }

    public @Nullable
    MovAvgModel.AbstractModelParser get(String parserName) {
        return movAvgParsers.get(parserName);
    }

    public Set<String> getAllNames() {
        return movAvgParsers.keySet();
    }

    private void add(Map<String, MovAvgModel.AbstractModelParser> map, MovAvgModel.AbstractModelParser parser) {
        map.put(parser.getName(), parser);
    }
}
