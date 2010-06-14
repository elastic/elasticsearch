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

package org.elasticsearch.index.query.xcontent;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.collect.ImmutableMap;
import org.elasticsearch.util.settings.Settings;

import javax.annotation.Nullable;
import java.util.Map;

import static org.elasticsearch.util.collect.Maps.*;

/**
 * @author kimchy (shay.banon)
 */
public class XContentQueryParserRegistry {

    private final Map<String, XContentQueryParser> queryParsers;

    private final Map<String, XContentFilterParser> filterParsers;

    public XContentQueryParserRegistry(Index index,
                                       @IndexSettings Settings indexSettings,
                                       AnalysisService analysisService,
                                       @Nullable Iterable<XContentQueryParser> queryParsers,
                                       @Nullable Iterable<XContentFilterParser> filterParsers) {

        Map<String, XContentQueryParser> queryParsersMap = newHashMap();
        // add defaults
        add(queryParsersMap, new DisMaxQueryParser(index, indexSettings));
        add(queryParsersMap, new MatchAllQueryParser(index, indexSettings));
        add(queryParsersMap, new QueryStringQueryParser(index, indexSettings, analysisService));
        add(queryParsersMap, new BoolQueryParser(index, indexSettings));
        add(queryParsersMap, new TermQueryParser(index, indexSettings));
        add(queryParsersMap, new FuzzyQueryParser(index, indexSettings));
        add(queryParsersMap, new FieldQueryParser(index, indexSettings, analysisService));
        add(queryParsersMap, new RangeQueryParser(index, indexSettings));
        add(queryParsersMap, new PrefixQueryParser(index, indexSettings));
        add(queryParsersMap, new WildcardQueryParser(index, indexSettings));
        add(queryParsersMap, new FilteredQueryParser(index, indexSettings));
        add(queryParsersMap, new ConstantScoreQueryParser(index, indexSettings));
        add(queryParsersMap, new CustomBoostFactorQueryParser(index, indexSettings));
        add(queryParsersMap, new CustomScoreQueryParser(index, indexSettings));
        add(queryParsersMap, new SpanTermQueryParser(index, indexSettings));
        add(queryParsersMap, new SpanNotQueryParser(index, indexSettings));
        add(queryParsersMap, new SpanFirstQueryParser(index, indexSettings));
        add(queryParsersMap, new SpanNearQueryParser(index, indexSettings));
        add(queryParsersMap, new SpanOrQueryParser(index, indexSettings));
        add(queryParsersMap, new MoreLikeThisQueryParser(index, indexSettings));
        add(queryParsersMap, new MoreLikeThisFieldQueryParser(index, indexSettings));
        add(queryParsersMap, new FuzzyLikeThisQueryParser(index, indexSettings));
        add(queryParsersMap, new FuzzyLikeThisFieldQueryParser(index, indexSettings));

        // now, copy over the ones provided
        if (queryParsers != null) {
            for (XContentQueryParser queryParser : queryParsers) {
                add(queryParsersMap, queryParser);
            }
        }
        this.queryParsers = ImmutableMap.copyOf(queryParsersMap);

        Map<String, XContentFilterParser> filterParsersMap = newHashMap();
        // add defaults
        add(filterParsersMap, new TermFilterParser(index, indexSettings));
        add(filterParsersMap, new TermsFilterParser(index, indexSettings));
        add(filterParsersMap, new RangeFilterParser(index, indexSettings));
        add(filterParsersMap, new PrefixFilterParser(index, indexSettings));
        add(filterParsersMap, new QueryFilterParser(index, indexSettings));
        add(filterParsersMap, new BoolFilterParser(index, indexSettings));
        add(filterParsersMap, new AndFilterParser(index, indexSettings));
        add(filterParsersMap, new OrFilterParser(index, indexSettings));
        add(filterParsersMap, new NotFilterParser(index, indexSettings));

        if (filterParsers != null) {
            for (XContentFilterParser filterParser : filterParsers) {
                add(filterParsersMap, filterParser);
            }
        }
        this.filterParsers = ImmutableMap.copyOf(filterParsersMap);
    }

    public XContentQueryParser queryParser(String name) {
        return queryParsers.get(name);
    }

    public XContentFilterParser filterParser(String name) {
        return filterParsers.get(name);
    }

    private void add(Map<String, XContentFilterParser> map, XContentFilterParser filterParser) {
        for (String name : filterParser.names()) {
            map.put(name.intern(), filterParser);
        }
    }

    private void add(Map<String, XContentQueryParser> map, XContentQueryParser queryParser) {
        for (String name : queryParser.names()) {
            map.put(name.intern(), queryParser);
        }
    }
}
