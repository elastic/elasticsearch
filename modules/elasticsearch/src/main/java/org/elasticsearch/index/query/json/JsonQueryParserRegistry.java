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

package org.elasticsearch.index.query.json;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.settings.Settings;

import java.util.Map;

import static com.google.common.collect.Maps.*;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonQueryParserRegistry {

    private final Map<String, JsonQueryParser> queryParsers;

    private final Map<String, JsonFilterParser> filterParsers;

    public JsonQueryParserRegistry(Index index,
                                   @IndexSettings Settings indexSettings,
                                   AnalysisService analysisService,
                                   @Nullable Iterable<JsonQueryParser> queryParsers,
                                   @Nullable Iterable<JsonFilterParser> filterParsers) {

        Map<String, JsonQueryParser> queryParsersMap = newHashMap();
        // add defaults
        add(queryParsersMap, new DisMaxJsonQueryParser(index, indexSettings));
        add(queryParsersMap, new MatchAllJsonQueryParser(index, indexSettings));
        add(queryParsersMap, new QueryStringJsonQueryParser(index, indexSettings, analysisService));
        add(queryParsersMap, new BoolJsonQueryParser(index, indexSettings));
        add(queryParsersMap, new TermJsonQueryParser(index, indexSettings));
        add(queryParsersMap, new RangeJsonQueryParser(index, indexSettings));
        add(queryParsersMap, new PrefixJsonQueryParser(index, indexSettings));
        add(queryParsersMap, new WildcardJsonQueryParser(index, indexSettings));
        add(queryParsersMap, new FilteredQueryJsonQueryParser(index, indexSettings));
        add(queryParsersMap, new ConstantScoreQueryJsonQueryParser(index, indexSettings));
        add(queryParsersMap, new SpanTermJsonQueryParser(index, indexSettings));
        add(queryParsersMap, new SpanNotJsonQueryParser(index, indexSettings));
        add(queryParsersMap, new SpanFirstJsonQueryParser(index, indexSettings));
        add(queryParsersMap, new SpanNearJsonQueryParser(index, indexSettings));
        add(queryParsersMap, new SpanOrJsonQueryParser(index, indexSettings));

        // now, copy over the ones provided
        if (queryParsers != null) {
            for (JsonQueryParser queryParser : queryParsers) {
                add(queryParsersMap, queryParser);
            }
        }
        this.queryParsers = ImmutableMap.copyOf(queryParsersMap);

        Map<String, JsonFilterParser> filterParsersMap = newHashMap();
        // add defaults
        add(filterParsersMap, new TermJsonFilterParser(index, indexSettings));
        add(filterParsersMap, new RangeJsonFilterParser(index, indexSettings));
        add(filterParsersMap, new PrefixJsonFilterParser(index, indexSettings));
        add(filterParsersMap, new QueryJsonFilterParser(index, indexSettings));
        add(filterParsersMap, new BoolJsonFilterParser(index, indexSettings));

        if (filterParsers != null) {
            for (JsonFilterParser filterParser : filterParsers) {
                add(filterParsersMap, filterParser);
            }
        }
        this.filterParsers = ImmutableMap.copyOf(filterParsersMap);
    }

    public JsonQueryParser queryParser(String name) {
        return queryParsers.get(name);
    }

    public JsonFilterParser filterParser(String name) {
        return filterParsers.get(name);
    }

    private void add(Map<String, JsonFilterParser> map, JsonFilterParser filterParser) {
        map.put(filterParser.name(), filterParser);
    }

    private void add(Map<String, JsonQueryParser> map, JsonQueryParser jsonQueryParser) {
        map.put(jsonQueryParser.name(), jsonQueryParser);
    }
}
