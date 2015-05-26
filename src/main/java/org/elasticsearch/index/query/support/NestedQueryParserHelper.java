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

package org.elasticsearch.index.query.support;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SubSearchContext;

import java.io.IOException;

/**
 * A helper that helps with parsing inner queries of the nested query.
 * 1) Takes into account that type nested path can appear before or after the inner query
 * 2) Updates the {@link NestedScope} when parsing the inner query.
 */
public class NestedQueryParserHelper {
    
    public static final ScoreMode DEFAULT_SCORE_MODE = ScoreMode.Avg;

    private final QueryParseContext parseContext;
    
    private BytesReference source;
    private Query innerQuery;
    private String path;

    private BitDocIdSetFilter parentFilter;
    private BitDocIdSetFilter childFilter;

    private DocumentMapper childDocumentMapper;
    private ObjectMapper nestedObjectMapper;
    private ObjectMapper parentObjectMapper;
    
    private ScoreMode scoreMode;
    private Tuple<String, SubSearchContext> innerHits;

    public NestedQueryParserHelper(XContentParser parser, SearchContext searchContext) {
        parseContext = searchContext.queryParserService().getParseContext();
        parseContext.reset(parser);
    }

    public NestedQueryParserHelper(QueryParseContext parseContext) {
        this.parseContext = parseContext;
    }
    
    public void initializeFromContext() throws IOException {
        source = XContentFactory.smileBuilder().copyCurrentStructure(parseContext.parser()).bytes();
    }

    public void parseInnerQuery() throws IOException {
        if (path == null) {
            throw new QueryParsingException(parseContext, "[nested] requires 'path' field");
        } else if (source == null) {
            throw new QueryParsingException(parseContext, "[nested] query/filter source has not been initialized");
        }
        XContentParser old = parseContext.parser();
        try {
            XContentParser innerParser = XContentHelper.createParser(source);
            parseContext.parser(innerParser);
            setPathLevel();
            try {
                innerQuery = parseContext.parseInnerQuery();
            } finally {
                resetPathLevel();
            }
        } finally {
            parseContext.parser(old);
        }
    }   

    public void parseInnerFilter() throws IOException {
        if (path == null) {
            throw new QueryParsingException(parseContext, "[nested] requires 'path' field");
        } else if (source == null) {
            throw new QueryParsingException(parseContext, "[nested] query/filter source has not been initialized");
        }
        XContentParser old = parseContext.parser();
        try {
            XContentParser innerParser = XContentHelper.createParser(source);
            parseContext.parser(innerParser);
            setPathLevel();
            try {
                innerQuery = new ConstantScoreQuery(parseContext.parseInnerFilter());
            } finally {
                resetPathLevel();
            }
        } finally {
            parseContext.parser(old);
        }
    }
    
    public boolean safeParseInnerFilter() {
        try {
            parseInnerFilter();
            return true;
        } catch (QueryParsingException e) {
            return false;
        } catch (IOException e) {
            return false;
        }
    }
    
    public Query getInnerQuery() {
        return innerQuery;
    }
    
    public void setInnerQuery(Query innerQuery) {
        this.innerQuery = innerQuery;
    }

    public void setPath(String path) {
        this.path = path;
        MapperService.SmartNameObjectMapper smart = parseContext.smartObjectMapper(path);
        if (smart == null) {
            throw new QueryParsingException(parseContext, "[nested] failed to find nested object under path [" + path + "]");
        }
        childDocumentMapper = smart.docMapper();
        nestedObjectMapper = smart.mapper();
        if (nestedObjectMapper == null) {
            throw new QueryParsingException(parseContext, "[nested] failed to find nested object under path [" + path + "]");
        }
        if (!nestedObjectMapper.nested().isNested()) {
            throw new QueryParsingException(parseContext, "[nested] nested object under path [" + path + "] is not of nested type");
        }
    }

    public String getPath() {
        return path;
    }

    public void setScoreMode(ScoreMode scoreMode) {
        this.scoreMode = scoreMode;
    }

    public void setInnerHits(Tuple<String, SubSearchContext> innerHits) {
        this.innerHits = innerHits;
    }
    
    public ObjectMapper getNestedObjectMapper() {
        return nestedObjectMapper;
    }

    public ObjectMapper getParentObjectMapper() {
        return parentObjectMapper;
    }

    public void setPathLevel() {
        ObjectMapper objectMapper = parseContext.nestedScope().getObjectMapper();
        if (objectMapper == null) {
            parentFilter = parseContext.bitsetFilter(Queries.newNonNestedFilter());
        } else {
            parentFilter = parseContext.bitsetFilter(objectMapper.nestedTypeFilter());
        }
        childFilter = parseContext.bitsetFilter(nestedObjectMapper.nestedTypeFilter());
        parentObjectMapper = parseContext.nestedScope().nextLevel(nestedObjectMapper);
    }

    public void resetPathLevel() {
        parseContext.nestedScope().previousLevel();
    }

    public ToParentBlockJoinQuery toParentBlockJoinQuery() { 
        if (innerHits != null) {
            InnerHitsContext.NestedInnerHits nestedInnerHits = 
                new InnerHitsContext.NestedInnerHits(innerHits.v2(), innerQuery, null, parentObjectMapper, nestedObjectMapper);
            String name = innerHits.v1() != null ? innerHits.v1() : path;
            parseContext.addInnerHits(name, nestedInnerHits);
        }

        if (innerQuery != null) {
            ScoreMode scoreMode = DEFAULT_SCORE_MODE;
            if (this.scoreMode != null) {
                scoreMode = this.scoreMode;
            }        
            return new ToParentBlockJoinQuery(Queries.filtered(innerQuery, childFilter), parentFilter, scoreMode);
        } else {
            return null;
        }
    }   
    
    public ScoreMode getScoreMode(String scoreMode) {
        switch (scoreMode) {
            case "avg":
                return ScoreMode.Avg;
            case "max":
                return ScoreMode.Max;
            case "total":
            case "sum":
                return ScoreMode.Total;
            case "none":
                return ScoreMode.None;
            default:
                throw new QueryParsingException(parseContext, "illegal score_mode for nested query [" + scoreMode + "]");
        }
    }
}
