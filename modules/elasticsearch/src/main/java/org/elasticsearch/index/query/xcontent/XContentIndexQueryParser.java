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

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.util.ThreadLocals;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.util.guice.inject.assistedinject.Assisted;
import org.elasticsearch.util.io.FastByteArrayOutputStream;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.xcontent.XContentFactory;
import org.elasticsearch.util.xcontent.XContentParser;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.util.collect.Lists.*;

/**
 * @author kimchy (shay.banon)
 */
public class XContentIndexQueryParser extends AbstractIndexComponent implements IndexQueryParser {

    public static final class Defaults {
        public static final String QUERY_PREFIX = "index.queryparser.query";
        public static final String FILTER_PREFIX = "index.queryparser.filter";
    }

    private ThreadLocal<ThreadLocals.CleanableValue<QueryParseContext>> cache = new ThreadLocal<ThreadLocals.CleanableValue<QueryParseContext>>() {
        @Override protected ThreadLocals.CleanableValue<QueryParseContext> initialValue() {
            return new ThreadLocals.CleanableValue<QueryParseContext>(new QueryParseContext(index, queryParserRegistry, mapperService, similarityService, indexCache));
        }
    };

    private final String name;

    private final MapperService mapperService;

    private final SimilarityService similarityService;

    private final IndexCache indexCache;

    private final XContentQueryParserRegistry queryParserRegistry;

    @Inject public XContentIndexQueryParser(Index index,
                                            @IndexSettings Settings indexSettings,
                                            MapperService mapperService, IndexCache indexCache,
                                            AnalysisService analysisService, @Nullable SimilarityService similarityService,
                                            @Nullable Map<String, XContentQueryParserFactory> namedQueryParsers,
                                            @Nullable Map<String, XContentFilterParserFactory> namedFilterParsers,
                                            @Assisted String name, @Assisted @Nullable Settings settings) {
        super(index, indexSettings);
        this.name = name;
        this.mapperService = mapperService;
        this.similarityService = similarityService;
        this.indexCache = indexCache;

        List<XContentQueryParser> queryParsers = newArrayList();
        if (namedQueryParsers != null) {
            Map<String, Settings> queryParserGroups = indexSettings.getGroups(XContentIndexQueryParser.Defaults.QUERY_PREFIX);
            for (Map.Entry<String, XContentQueryParserFactory> entry : namedQueryParsers.entrySet()) {
                String queryParserName = entry.getKey();
                XContentQueryParserFactory queryParserFactory = entry.getValue();
                Settings queryParserSettings = queryParserGroups.get(queryParserName);

                queryParsers.add(queryParserFactory.create(queryParserName, queryParserSettings));
            }
        }

        List<XContentFilterParser> filterParsers = newArrayList();
        if (namedFilterParsers != null) {
            Map<String, Settings> filterParserGroups = indexSettings.getGroups(XContentIndexQueryParser.Defaults.FILTER_PREFIX);
            for (Map.Entry<String, XContentFilterParserFactory> entry : namedFilterParsers.entrySet()) {
                String filterParserName = entry.getKey();
                XContentFilterParserFactory filterParserFactory = entry.getValue();
                Settings filterParserSettings = filterParserGroups.get(filterParserName);

                filterParsers.add(filterParserFactory.create(filterParserName, filterParserSettings));
            }
        }

        this.queryParserRegistry = new XContentQueryParserRegistry(index, indexSettings, analysisService, queryParsers, filterParsers);
    }

    @Override public String name() {
        return this.name;
    }

    public XContentQueryParserRegistry queryParserRegistry() {
        return this.queryParserRegistry;
    }

    @Override public Query parse(QueryBuilder queryBuilder) throws ElasticSearchException {
        XContentParser parser = null;
        try {
            FastByteArrayOutputStream unsafeBytes = queryBuilder.buildAsUnsafeBytes();
            parser = XContentFactory.xContent(unsafeBytes.unsafeByteArray(), 0, unsafeBytes.size()).createParser(unsafeBytes.unsafeByteArray(), 0, unsafeBytes.size());
            return parse(cache.get().get(), parser);
        } catch (QueryParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryParsingException(index, "Failed to parse", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    @Override public Query parse(byte[] source) throws ElasticSearchException {
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            return parse(cache.get().get(), parser);
        } catch (QueryParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryParsingException(index, "Failed to parse", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    @Override public Query parse(String source) throws QueryParsingException {
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            return parse(cache.get().get(), parser);
        } catch (QueryParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryParsingException(index, "Failed to parse [" + source + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public Query parse(XContentParser parser) {
        try {
            return parse(cache.get().get(), parser);
        } catch (IOException e) {
            throw new QueryParsingException(index, "Failed to parse", e);
        }
    }

    private Query parse(QueryParseContext parseContext, XContentParser parser) throws IOException, QueryParsingException {
        parseContext.reset(parser);
        return parseContext.parseInnerQuery();
    }
}
