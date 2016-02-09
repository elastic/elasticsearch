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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * A helper that helps with parsing inner queries of the nested query.
 * 1) Takes into account that type nested path can appear before or after the inner query
 * 2) Updates the {@link NestedScope} when parsing the inner query.
 */
public class NestedInnerQueryParseSupport {

    protected final QueryShardContext shardContext;
    protected final QueryParseContext parseContext;

    private BytesReference source;
    private Query innerQuery;
    private Query innerFilter;
    protected String path;

    private boolean filterParsed = false;
    private boolean queryParsed = false;
    protected boolean queryFound = false;
    protected boolean filterFound = false;

    protected BitSetProducer parentFilter;
    protected Query childFilter;

    protected ObjectMapper nestedObjectMapper;
    private ObjectMapper parentObjectMapper;

    public NestedInnerQueryParseSupport(XContentParser parser, SearchContext searchContext) {
        shardContext = searchContext.getQueryShardContext();
        parseContext = shardContext.parseContext();
        shardContext.reset(parser);

    }

    public NestedInnerQueryParseSupport(QueryShardContext context) {
        this.parseContext = context.parseContext();
        this.shardContext = context;
    }

    public void query() throws IOException {
        if (path != null) {
            setPathLevel();
            try {
                innerQuery = parseContext.parseInnerQueryBuilder().toQuery(this.shardContext);
            } finally {
                resetPathLevel();
            }
            queryParsed = true;
        } else {
            source = XContentFactory.smileBuilder().copyCurrentStructure(parseContext.parser()).bytes();
        }
        queryFound = true;
    }

    public void filter() throws IOException {
        if (path != null) {
            setPathLevel();
            try {
                innerFilter = parseContext.parseInnerQueryBuilder().toFilter(this.shardContext);
            } finally {
                resetPathLevel();
            }
            filterParsed = true;
        } else {
            source = XContentFactory.smileBuilder().copyCurrentStructure(parseContext.parser()).bytes();
        }
        filterFound = true;
    }

    public Query getInnerQuery() throws IOException {
        if (queryParsed) {
            return innerQuery;
        } else {
            if (path == null) {
                throw new QueryShardException(shardContext, "[nested] requires 'path' field");
            }
            if (!queryFound) {
                throw new QueryShardException(shardContext, "[nested] requires either 'query' or 'filter' field");
            }

            XContentParser old = parseContext.parser();
            try {
                XContentParser innerParser = XContentHelper.createParser(source);
                parseContext.parser(innerParser);
                setPathLevel();
                try {
                    innerQuery = parseContext.parseInnerQueryBuilder().toQuery(this.shardContext);
                } finally {
                    resetPathLevel();
                }
                queryParsed = true;
                return innerQuery;
            } finally {
                parseContext.parser(old);
            }
        }
    }

    public Query getInnerFilter() throws IOException {
        if (filterParsed) {
            return innerFilter;
        } else {
            if (path == null) {
                throw new QueryShardException(shardContext, "[nested] requires 'path' field");
            }
            if (!filterFound) {
                throw new QueryShardException(shardContext, "[nested] requires either 'query' or 'filter' field");
            }

            setPathLevel();
            XContentParser old = parseContext.parser();
            try {
                XContentParser innerParser = XContentHelper.createParser(source);
                parseContext.parser(innerParser);
                innerFilter = parseContext.parseInnerQueryBuilder().toFilter(this.shardContext);
                filterParsed = true;
                return innerFilter;
            } finally {
                resetPathLevel();
                parseContext.parser(old);
            }
        }
    }

    public void setPath(String path) {
        this.path = path;
        nestedObjectMapper = shardContext.getObjectMapper(path);
        if (nestedObjectMapper == null) {
            throw new QueryShardException(shardContext, "[nested] failed to find nested object under path [" + path + "]");
        }
        if (!nestedObjectMapper.nested().isNested()) {
            throw new QueryShardException(shardContext, "[nested] nested object under path [" + path + "] is not of nested type");
        }
    }

    public String getPath() {
        return path;
    }

    public ObjectMapper getNestedObjectMapper() {
        return nestedObjectMapper;
    }

    public boolean queryFound() {
        return queryFound;
    }

    public boolean filterFound() {
        return filterFound;
    }

    public ObjectMapper getParentObjectMapper() {
        return parentObjectMapper;
    }

    private void setPathLevel() {
        ObjectMapper objectMapper = shardContext.nestedScope().getObjectMapper();
        if (objectMapper == null) {
            parentFilter = shardContext.bitsetFilter(Queries.newNonNestedFilter());
        } else {
            parentFilter = shardContext.bitsetFilter(objectMapper.nestedTypeFilter());
        }
        childFilter = nestedObjectMapper.nestedTypeFilter();
        parentObjectMapper = shardContext.nestedScope().nextLevel(nestedObjectMapper);
    }

    private void resetPathLevel() {
        shardContext.nestedScope().previousLevel();
    }

}
