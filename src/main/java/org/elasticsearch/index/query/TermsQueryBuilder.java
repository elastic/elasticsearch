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
package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.lucene.search.Queries.fixNegativeQueryIfNeeded;

/**
 * <pre>
 * "terms" : {
 *  "field_name" : [ "value1", "value2" ]
 *  "minimum_match" : 1
 * }
 * </pre>
 */
public class TermsQueryBuilder extends BaseQueryBuilder implements QueryParser, BoostableQueryBuilder<TermsQueryBuilder> {

    private final String name;

    private final Object[] values;

    private String minimumShouldMatch;

    private Boolean disableCoord;

    private float boost = -1;

    private String queryName;
    
    public static final String NAME = "terms";

    @Inject
    public TermsQueryBuilder() {
        this(null, Strings.EMPTY_ARRAY);
    }

    /**
     * A query for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, String... values) {
        this(name, (Object[]) values);
    }

    /**
     * A query for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, int... values) {
        this.name = name;
        this.values = new Integer[values.length];
        for (int i = 0; i < values.length; i++) {
            this.values[i] = values[i];
        }
    }

    /**
     * A query for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, long... values) {
        this.name = name;
        this.values = new Long[values.length];
        for (int i = 0; i < values.length; i++) {
            this.values[i] = values[i];
        }
    }

    /**
     * A query for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, float... values) {
        this.name = name;
        this.values = new Float[values.length];
        for (int i = 0; i < values.length; i++) {
            this.values[i] = values[i];
        }
    }

    /**
     * A query for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, double... values) {
        this.name = name;
        this.values = new Double[values.length];
        for (int i = 0; i < values.length; i++) {
            this.values[i] = values[i];
        }
    }

    /**
     * A query for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, Object... values) {
        this.name = name;
        this.values = values;
    }

  /**
   * A query for a field based on several terms matching on any of them.
   *
   * @param name    The field name
   * @param values  The terms
   */
    public TermsQueryBuilder(String name, Collection values) {
        this(name, values.toArray());
    }

    /**
     * Sets the minimum number of matches across the provided terms. Defaults to <tt>1</tt>.
     */
    public TermsQueryBuilder minimumMatch(int minimumMatch) {
        this.minimumShouldMatch = Integer.toString(minimumMatch);
        return this;
    }

    public TermsQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public TermsQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Disables <tt>Similarity#coord(int,int)</tt> in scoring. Defualts to <tt>false</tt>.
     */
    public TermsQueryBuilder disableCoord(boolean disableCoord) {
        this.disableCoord = disableCoord;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public TermsQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TermsQueryBuilder.NAME);
        builder.startArray(name);
        for (Object value : values) {
            builder.value(value);
        }
        builder.endArray();

        if (minimumShouldMatch != null) {
            builder.field("minimum_should_match", minimumShouldMatch);
        }
        if (disableCoord != null) {
            builder.field("disable_coord", disableCoord);
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }

        builder.endObject();
    }
    
    @Override
    public String[] names() {
        return new String[]{NAME, "in"}; // allow both "in" and "terms" (since its similar to the "terms" filter)
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;
        boolean disableCoord = false;
        float boost = 1.0f;
        String minimumShouldMatch = null;
        List<Object> values = newArrayList();
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if  (fieldName != null) {
                    throw new QueryParsingException(parseContext.index(), "[terms] query does not support multiple fields");
                }
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    Object value = parser.objectBytes();
                    if (value == null) {
                        throw new QueryParsingException(parseContext.index(), "No value specified for terms query");
                    }
                    values.add(value);
                }
            } else if (token.isValue()) {
                if ("disable_coord".equals(currentFieldName) || "disableCoord".equals(currentFieldName)) {
                    disableCoord = parser.booleanValue();
                } else if ("minimum_match".equals(currentFieldName) || "minimumMatch".equals(currentFieldName)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[terms] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new QueryParsingException(parseContext.index(), "[terms] query does not support [" + currentFieldName + "]");
            }
        }

        if (fieldName == null) {
            throw new QueryParsingException(parseContext.index(), "No field specified for terms query");
        }

        FieldMapper mapper = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        String[] previousTypes = null;
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            mapper = smartNameFieldMappers.mapper();
        }


        BooleanQuery booleanQuery = new BooleanQuery(disableCoord);
        for (Object value : values) {
            if (mapper != null) {
                booleanQuery.add(new BooleanClause(mapper.termQuery(value, parseContext), BooleanClause.Occur.SHOULD));
            } else {
                booleanQuery.add(new TermQuery(new Term(fieldName, BytesRefs.toString(value))), BooleanClause.Occur.SHOULD);
            }
        }
        booleanQuery.setBoost(boost);
        Queries.applyMinimumShouldMatch(booleanQuery, minimumShouldMatch);
        Query query = fixNegativeQueryIfNeeded(booleanQuery);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
