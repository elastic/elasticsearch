/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.percolator;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastByteArrayOutputStream;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.IndexQueryParser;
import org.elasticsearch.index.query.IndexQueryParserMissingException;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.settings.IndexSettings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.SourceToParse.*;

/**
 * @author kimchy (shay.banon)
 */
public class PercolatorService extends AbstractIndexComponent {

    public static class Request {
        private final String type;
        private final byte[] source;

        private String match;
        private String unmatch;

        public Request(String type, byte[] source) {
            this.type = type;
            this.source = source;
        }

        public String type() {
            return type;
        }

        public byte[] source() {
            return source;
        }

        public String match() {
            return this.match;
        }

        public Request match(String match) {
            this.match = match;
            return this;
        }

        public String unmatch() {
            return this.unmatch;
        }

        public Request unmatch(String unmatch) {
            this.unmatch = unmatch;
            return this;
        }
    }

    public static final class Response {
        private final List<String> matches;
        private final boolean mappersAdded;

        public Response(List<String> matches, boolean mappersAdded) {
            this.matches = matches;
            this.mappersAdded = mappersAdded;
        }

        public boolean mappersAdded() {
            return this.mappersAdded;
        }

        public List<String> matches() {
            return matches;
        }
    }

    private final MapperService mapperService;

    private final IndexQueryParserService queryParserService;

    private volatile ImmutableMap<String, Query> queries = ImmutableMap.of();

    @Inject public PercolatorService(Index index, @IndexSettings Settings indexSettings,
                                     MapperService mapperService, IndexQueryParserService queryParserService) {
        super(index, indexSettings);
        this.mapperService = mapperService;
        this.queryParserService = queryParserService;
    }

    public void addQuery(String name, QueryBuilder queryBuilder) {
        addQuery(name, null, queryBuilder);
    }

    public void addQuery(String name, @Nullable String queryParserName, QueryBuilder queryBuilder) {
        FastByteArrayOutputStream unsafeBytes = queryBuilder.buildAsUnsafeBytes();
        addQuery(name, queryParserName, unsafeBytes.unsafeByteArray(), 0, unsafeBytes.size());
    }

    public void addQuery(String name, @Nullable String queryParserName,
                         byte[] querySource, int querySourceOffset, int querySourceLength) throws ElasticSearchException {
        IndexQueryParser queryParser = queryParserService.defaultIndexQueryParser();
        if (queryParserName != null) {
            queryParser = queryParserService.indexQueryParser(queryParserName);
            if (queryParser == null) {
                throw new IndexQueryParserMissingException(queryParserName);
            }
        }

        Query query = queryParser.parse(querySource, querySourceOffset, querySourceLength).query();
        addQuery(name, query);
    }

    public synchronized void addQuery(String name, Query query) {
        this.queries = MapBuilder.newMapBuilder(queries).put(name, query).immutableMap();
    }

    public synchronized void removeQuery(String name) {
        this.queries = MapBuilder.newMapBuilder(queries).remove(name).immutableMap();
    }

    public Response percolate(Request request) throws ElasticSearchException {
        // first, parse the source doc into a MemoryIndex
        final MemoryIndex memoryIndex = new MemoryIndex();
        DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(request.type());
        ParsedDocument doc = docMapper.parse(source(request.source()).type(request.type()).flyweight(true));

        for (Fieldable field : doc.doc().getFields()) {
            if (!field.isIndexed()) {
                continue;
            }
            TokenStream tokenStream = field.tokenStreamValue();
            if (tokenStream != null) {
                memoryIndex.addField(field.name(), tokenStream, field.getBoost());
            } else {
                Reader reader = field.readerValue();
                if (reader != null) {
                    try {
                        memoryIndex.addField(field.name(), doc.analyzer().reusableTokenStream(field.name(), reader), field.getBoost() * doc.doc().getBoost());
                    } catch (IOException e) {
                        throw new MapperParsingException("Failed to analyze field [" + field.name() + "]", e);
                    }
                } else {
                    String value = field.stringValue();
                    if (value != null) {
                        try {
                            memoryIndex.addField(field.name(), doc.analyzer().reusableTokenStream(field.name(), new FastStringReader(value)), field.getBoost() * doc.doc().getBoost());
                        } catch (IOException e) {
                            throw new MapperParsingException("Failed to analyze field [" + field.name() + "]", e);
                        }
                    }
                }
            }
        }

        Lucene.ExistsCollector collector = new Lucene.ExistsCollector();
        List<String> matches = new ArrayList<String>();
        IndexSearcher searcher = memoryIndex.createSearcher();
        for (Map.Entry<String, Query> entry : queries.entrySet()) {
            if (request.match() != null) {
                if (!Regex.simpleMatch(request.match(), entry.getKey())) {
                    continue;
                }
            }
            if (request.unmatch() != null) {
                if (Regex.simpleMatch(request.unmatch(), entry.getKey())) {
                    continue;
                }
            }

            try {
                searcher.search(entry.getValue(), collector);
            } catch (IOException e) {
                logger.warn("[" + entry.getKey() + "] failed to execute query", e);
            }

            if (collector.exists()) {
                matches.add(entry.getKey());
            }
        }

        return new Response(matches, doc.mappersAdded());
    }
}
