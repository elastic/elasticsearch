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

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.io.FastStringReader;
import org.elasticsearch.util.json.Jackson;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.*;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonIndexQueryParser extends AbstractIndexComponent implements IndexQueryParser {

    public static final class Defaults {
        public static final String JSON_QUERY_PREFIX = "index.queryparser.json.query";
        public static final String JSON_FILTER_PREFIX = "index.queryparser.json.filter";
    }

    private ThreadLocal<JsonQueryParseContext> cache = new ThreadLocal<JsonQueryParseContext>() {
        @Override protected JsonQueryParseContext initialValue() {
            return new JsonQueryParseContext(index, queryParserRegistry, mapperService, filterCache);
        }
    };

    private final JsonFactory jsonFactory = Jackson.defaultJsonFactory();

    private final String name;

    private final MapperService mapperService;

    private final FilterCache filterCache;

    private final JsonQueryParserRegistry queryParserRegistry;

    @Inject public JsonIndexQueryParser(Index index,
                                        @IndexSettings Settings indexSettings,
                                        MapperService mapperService, FilterCache filterCache,
                                        AnalysisService analysisService,
                                        @Nullable Map<String, JsonQueryParserFactory> jsonQueryParsers,
                                        @Nullable Map<String, JsonFilterParserFactory> jsonFilterParsers,
                                        @Assisted String name, @Assisted @Nullable Settings settings) {
        super(index, indexSettings);
        this.name = name;
        this.mapperService = mapperService;
        this.filterCache = filterCache;

        List<JsonQueryParser> queryParsers = newArrayList();
        if (jsonQueryParsers != null) {
            Map<String, Settings> jsonQueryParserGroups = indexSettings.getGroups(JsonIndexQueryParser.Defaults.JSON_QUERY_PREFIX);
            for (Map.Entry<String, JsonQueryParserFactory> entry : jsonQueryParsers.entrySet()) {
                String queryParserName = entry.getKey();
                JsonQueryParserFactory queryParserFactory = entry.getValue();
                Settings queryParserSettings = jsonQueryParserGroups.get(queryParserName);

                queryParsers.add(queryParserFactory.create(queryParserName, queryParserSettings));
            }
        }

        List<JsonFilterParser> filterParsers = newArrayList();
        if (jsonFilterParsers != null) {
            Map<String, Settings> jsonFilterParserGroups = indexSettings.getGroups(JsonIndexQueryParser.Defaults.JSON_FILTER_PREFIX);
            for (Map.Entry<String, JsonFilterParserFactory> entry : jsonFilterParsers.entrySet()) {
                String filterParserName = entry.getKey();
                JsonFilterParserFactory filterParserFactory = entry.getValue();
                Settings filterParserSettings = jsonFilterParserGroups.get(filterParserName);

                filterParsers.add(filterParserFactory.create(filterParserName, filterParserSettings));
            }
        }

        this.queryParserRegistry = new JsonQueryParserRegistry(index, indexSettings, analysisService, queryParsers, filterParsers);
    }

    @Override public String name() {
        return this.name;
    }

    public JsonQueryParserRegistry queryParserRegistry() {
        return this.queryParserRegistry;
    }

    @Override public Query parse(QueryBuilder queryBuilder) throws ElasticSearchException {
        return parse(queryBuilder.build());
    }

    @Override public Query parse(String source) throws QueryParsingException {
        JsonParser jp = null;
        try {
            jp = jsonFactory.createJsonParser(new FastStringReader(source));
            return parse(cache.get(), source, jp);
        } catch (QueryParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryParsingException(index, "Failed to parse [" + source + "]", e);
        } finally {
            if (jp != null) {
                try {
                    jp.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    public Query parse(JsonParser jsonParser, String source) {
        try {
            return parse(cache.get(), source, jsonParser);
        } catch (IOException e) {
            throw new QueryParsingException(index, "Failed to parse [" + source + "]", e);
        }
    }

    private Query parse(JsonQueryParseContext parseContext, String source, JsonParser jsonParser) throws IOException, QueryParsingException {
        parseContext.reset(jsonParser);
        return parseContext.parseInnerQuery();
    }
}
