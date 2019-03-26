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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.search.MatchQuery;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.query.MatchQueryBuilder.OPERATOR_FIELD;

/**
 * The boolean prefix query analyzes the input text and creates a boolean query containing a Term query for each term, except
 * for the last term, which is used to create a prefix query
 */
public class MatchBoolPrefixQueryBuilder extends AbstractQueryBuilder<MatchBoolPrefixQueryBuilder> {

    public static final String NAME = "match_bool_prefix";

    private static final Operator DEFAULT_OPERATOR = Operator.OR;

    private final String fieldName;

    private final Object value;

    private String analyzer;

    private Operator operator = DEFAULT_OPERATOR;

    private String minimumShouldMatch;

    public MatchBoolPrefixQueryBuilder(String fieldName, Object value) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("[" + NAME + "] requires fieldName");
        }
        if (value == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query value");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    public MatchBoolPrefixQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        value = in.readGenericValue();
        analyzer = in.readOptionalString();
        operator = Operator.readFromStream(in);
        minimumShouldMatch = in.readOptionalString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(value);
        out.writeOptionalString(analyzer);
        operator.writeTo(out);
        out.writeOptionalString(minimumShouldMatch);
    }

    /** Returns the field name used in this query. */
    public String fieldName() {
        return this.fieldName;
    }

    /** Returns the value used in this query. */
    public Object value() {
        return this.value;
    }

    /** Get the analyzer to use, if previously set, otherwise {@code null} */
    public String analyzer() {
        return this.analyzer;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping
     * config for the field, or, if not set, the default search analyzer.
     */
    public MatchBoolPrefixQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /** Sets the operator to use when using a boolean query. Defaults to {@code OR}. */
    public MatchBoolPrefixQueryBuilder operator(Operator operator) {
        if (operator == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires operator to be non-null");
        }
        this.operator = operator;
        return this;
    }

    /** Returns the operator to use in a boolean query.*/
    public Operator operator() {
        return this.operator;
    }

    /** Sets optional minimumShouldMatch value to apply to the query */
    public MatchBoolPrefixQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /** Gets the minimumShouldMatch value */
    public String minimumShouldMatch() {
        return this.minimumShouldMatch;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(MatchQueryBuilder.QUERY_FIELD.getPreferredName(), value);
        if (analyzer != null) {
            builder.field(MatchQueryBuilder.ANALYZER_FIELD.getPreferredName(), analyzer);
        }
        builder.field(OPERATOR_FIELD.getPreferredName(), operator.toString());
        if (minimumShouldMatch != null) {
            builder.field(MatchQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), minimumShouldMatch);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    public static MatchBoolPrefixQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String analyzer = null;
        Operator operator = DEFAULT_OPERATOR;
        String minimumShouldMatch = null;
        String queryName = null;
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (MatchQueryBuilder.QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = parser.objectText();
                        } else if (MatchQueryBuilder.ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            analyzer = parser.text();
                        } else if (OPERATOR_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            operator = Operator.fromString(parser.text());
                        } else if (MatchQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            minimumShouldMatch = parser.textOrNull();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                "[" + NAME + "] query does not support [" + currentFieldName + "]");
                        }
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                            "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                value = parser.objectText();
            }
        }

        MatchBoolPrefixQueryBuilder queryBuilder = new MatchBoolPrefixQueryBuilder(fieldName, value);
        queryBuilder.analyzer(analyzer);
        queryBuilder.operator(operator);
        queryBuilder.minimumShouldMatch(minimumShouldMatch);
        queryBuilder.boost(boost);
        queryBuilder.queryName(queryName);
        return queryBuilder;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (analyzer != null && context.getIndexAnalyzers().get(analyzer) == null) {
            throw new QueryShardException(context, "[" + NAME + "] analyzer [" + analyzer + "] not found");
        }

        final MatchQuery matchQuery = new MatchQuery(context);
        if (analyzer != null) {
            matchQuery.setAnalyzer(analyzer);
        }
        matchQuery.setOccur(operator.toBooleanClauseOccur());

        final Query query = matchQuery.parse(MatchQuery.Type.BOOLEAN_PREFIX, fieldName, value);
        return Queries.maybeApplyMinimumShouldMatch(query, minimumShouldMatch);
    }

    @Override
    protected boolean doEquals(MatchBoolPrefixQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
            Objects.equals(value, other.value) &&
            Objects.equals(analyzer, other.analyzer) &&
            Objects.equals(operator, other.operator) &&
            Objects.equals(minimumShouldMatch, other.minimumShouldMatch);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value, analyzer, operator, minimumShouldMatch);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
