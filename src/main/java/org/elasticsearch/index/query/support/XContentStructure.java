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
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;

/**
 * XContentStructure is a class used to capture a subset of query, to be parsed
 * at a later time when more information (in this case, types) is available.
 * Note that using this class requires copying the parser's data, which will
 * result in additional overhead versus parsing the inner query/filter
 * immediately, however, the extra overhead means that the type not be
 * extracted prior to query parsing (in the case of unordered JSON).
 */
public abstract class XContentStructure {

    private final QueryParseContext parseContext;
    private BytesReference innerBytes;

    /**
     * Create a new XContentStructure for the current parsing context.
     */
    public XContentStructure(QueryParseContext queryParseContext) {
        this.parseContext = queryParseContext;
    }

    /**
     * "Freeze" the parsing content, which means copying the current parser's
     * structure into an internal {@link BytesReference} to be parsed later.
     * @return the original XContentStructure object
     */
    public XContentStructure freeze() throws IOException {
        this.bytes(XContentFactory.smileBuilder().copyCurrentStructure(parseContext.parser()).bytes());
        return this;
    }

    /**
     * Set the bytes to be used for parsing
     */
    public void bytes(BytesReference innerBytes) {
        this.innerBytes = innerBytes;
    }

    /**
     * Return the bytes that are going to be used for parsing
     */
    public BytesReference bytes() {
        return this.innerBytes;
    }

    /**
     * Use the captured bytes to parse the inner query using the specified
     * types. The original QueryParseContext's parser is switched during this
     * parsing, so this method is NOT thread-safe.
     * @param types types to be used during the inner query parsing
     * @return {@link Query} parsed from the bytes captured in {@code freeze()}
     */
    public Query asQuery(String... types) throws IOException {
        BytesReference br = this.bytes();
        assert br != null : "innerBytes must be set with .bytes(bytes) or .freeze() before parsing";
        XContentParser innerParser = XContentHelper.createParser(br);
        String[] origTypes = QueryParseContext.setTypesWithPrevious(types);
        XContentParser old = parseContext.parser();
        parseContext.parser(innerParser);
        try {
            return parseContext.parseInnerQuery();
        } finally {
            parseContext.parser(old);
            QueryParseContext.setTypes(origTypes);
        }
    }

    /**
     * Use the captured bytes to parse the inner filter using the specified
     * types. The original QueryParseContext's parser is switched during this
     * parsing, so this method is NOT thread-safe.
     * @param types types to be used during the inner filter parsing
     * @return {@link XConstantScoreQuery} wrapping the filter parsed from the bytes captured in {@code freeze()}
     */
    public Query asFilter(String... types) throws IOException {
        BytesReference br = this.bytes();
        assert br != null : "innerBytes must be set with .bytes(bytes) or .freeze() before parsing";
        XContentParser innerParser = XContentHelper.createParser(br);
        String[] origTypes = QueryParseContext.setTypesWithPrevious(types);
        XContentParser old = parseContext.parser();
        parseContext.parser(innerParser);
        try {
            Filter innerFilter = parseContext.parseInnerFilter();
            return new ConstantScoreQuery(innerFilter);
        } finally {
            parseContext.parser(old);
            QueryParseContext.setTypes(origTypes);
        }
    }

    /**
     * InnerQuery is an extension of {@code XContentStructure} that eagerly
     * parses the query in a streaming manner if the types are available at
     * construction time.
     */
    public static class InnerQuery extends XContentStructure {
        private Query query = null;
        private boolean queryParsed = false;
        public InnerQuery(QueryParseContext parseContext1, @Nullable String... types) throws IOException {
            super(parseContext1);
            if (types != null) {
                String[] origTypes = QueryParseContext.setTypesWithPrevious(types);
                try {
                    query = parseContext1.parseInnerQuery();
                    queryParsed = true;
                } finally {
                    QueryParseContext.setTypes(origTypes);
                }
            } else {
                BytesReference innerBytes = XContentFactory.smileBuilder().copyCurrentStructure(parseContext1.parser()).bytes();
                super.bytes(innerBytes);
            }
        }

        /**
         * Return the query represented by the XContentStructure object,
         * returning the cached Query if it has already been parsed.
         * @param types types to be used during the inner query parsing
         */
        @Override
        public Query asQuery(String... types) throws IOException {
            if (!queryParsed) { // query can be null
                this.query = super.asQuery(types);
            }
            return this.query;
        }
    }

    /**
     * InnerFilter is an extension of {@code XContentStructure} that eagerly
     * parses the filter in a streaming manner if the types are available at
     * construction time.
     */
    public static class InnerFilter extends XContentStructure {
        private Query query = null;
        private boolean queryParsed = false;


        public InnerFilter(QueryParseContext parseContext1, @Nullable String... types) throws IOException {
            super(parseContext1);
            if (types != null) {
                String[] origTypes = QueryParseContext.setTypesWithPrevious(types);
                try {
                    Filter innerFilter = parseContext1.parseInnerFilter();
                    query = new ConstantScoreQuery(innerFilter);
                    queryParsed = true;
                } finally {
                    QueryParseContext.setTypes(origTypes);
                }
            } else {
                BytesReference innerBytes = XContentFactory.smileBuilder().copyCurrentStructure(parseContext1.parser()).bytes();
                super.bytes(innerBytes);
            }
        }

        /**
         * Return the filter as an
         * {@link org.elasticsearch.common.lucene.search.XConstantScoreQuery}
         * represented by the XContentStructure object,
         * returning the cached Query if it has already been parsed.
         * @param types types to be used during the inner filter parsing
         */
        @Override
        public Query asFilter(String... types) throws IOException {
            if (!queryParsed) { // query can be null
                this.query = super.asFilter(types);
            }
            return this.query;
        }
    }
}
