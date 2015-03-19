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

import com.carrotsearch.hppc.ObjectFloatOpenHashMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.index.search.MultiMatchQuery;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Same as {@link MatchQueryBuilder} but supports multiple fields.
 */
public class MultiMatchQueryBuilder extends BaseQueryBuilder implements QueryParser, BoostableQueryBuilder<MultiMatchQueryBuilder> {

    public static final String NAME = "multi_match";

    private final Object text;

    private final List<String> fields;
    private ObjectFloatOpenHashMap<String> fieldsBoosts;

    private MultiMatchQueryBuilder.Type type;

    private MatchQueryBuilder.Operator operator;

    private String analyzer;

    private Float boost;

    private Integer slop;

    private Fuzziness fuzziness;

    private Integer prefixLength;

    private Integer maxExpansions;

    private String minimumShouldMatch;

    private String rewrite = null;

    private String fuzzyRewrite = null;

    private Boolean useDisMax;

    private Float tieBreaker;

    private Boolean lenient;

    private Float cutoffFrequency = null;

    private MatchQueryBuilder.ZeroTermsQuery zeroTermsQuery = null;

    private String queryName;


    public enum Type {

        /**
         * Uses the best matching boolean field as main score and uses
         * a tie-breaker to adjust the score based on remaining field matches
         */
        BEST_FIELDS(MatchQuery.Type.BOOLEAN, 0.0f, new ParseField("best_fields", "boolean")),

        /**
         * Uses the sum of the matching boolean fields to score the query
         */
        MOST_FIELDS(MatchQuery.Type.BOOLEAN, 1.0f, new ParseField("most_fields")),

        /**
         * Uses a blended DocumentFrequency to dynamically combine the queried
         * fields into a single field given the configured analysis is identical.
         * This type uses a tie-breaker to adjust the score based on remaining
         * matches per analyzed terms
         */
        CROSS_FIELDS(MatchQuery.Type.BOOLEAN, 0.0f, new ParseField("cross_fields")),

        /**
         * Uses the best matching phrase field as main score and uses
         * a tie-breaker to adjust the score based on remaining field matches
         */
        PHRASE(MatchQuery.Type.PHRASE, 0.0f, new ParseField("phrase")),

        /**
         * Uses the best matching phrase-prefix field as main score and uses
         * a tie-breaker to adjust the score based on remaining field matches
         */
        PHRASE_PREFIX(MatchQuery.Type.PHRASE_PREFIX, 0.0f, new ParseField("phrase_prefix"));

        private MatchQuery.Type matchQueryType;
        private final float tieBreaker;
        private final ParseField parseField;

        Type (MatchQuery.Type matchQueryType, float tieBreaker, ParseField parseField) {
            this.matchQueryType = matchQueryType;
            this.tieBreaker = tieBreaker;
            this.parseField = parseField;
        }

        public float tieBreaker() {
            return this.tieBreaker;
        }

        public MatchQuery.Type matchQueryType() {
            return matchQueryType;
        }

        public ParseField parseField() {
            return parseField;
        }

        public static Type parse(String value) {
            return parse(value, ParseField.EMPTY_FLAGS);
        }

        public static Type parse(String value, EnumSet<ParseField.Flag> flags) {
            MultiMatchQueryBuilder.Type[] values = MultiMatchQueryBuilder.Type.values();
            Type type = null;
            for (MultiMatchQueryBuilder.Type t : values) {
                if (t.parseField().match(value, flags)) {
                    type = t;
                    break;
                }
            }
            if (type == null) {
                throw new ElasticsearchParseException("No type found for value: " + value);
            }
            return type;
        }
    }

    @Inject
    public MultiMatchQueryBuilder() {
        this.text = null;
        this.fields = null;
    }

    @Override
    public String[] names() {
        return new String[]{
                NAME, "multiMatch"
        };
    }

    /**
     * Constructs a new text query.
     */
    public MultiMatchQueryBuilder(Object text, String... fields) {
        this.fields = Lists.newArrayList();
        this.fields.addAll(Arrays.asList(fields));
        this.text = text;
    }

    /**
     * Adds a field to run the multi match against.
     */
    public MultiMatchQueryBuilder field(String field) {
        fields.add(field);
        return this;
    }

    /**
     * Adds a field to run the multi match against with a specific boost.
     */
    public MultiMatchQueryBuilder field(String field, float boost) {
        fields.add(field);
        if (fieldsBoosts == null) {
            fieldsBoosts = new ObjectFloatOpenHashMap<>();
        }
        fieldsBoosts.put(field, boost);
        return this;
    }

    /**
     * Sets the type of the text query.
     */
    public MultiMatchQueryBuilder type(MultiMatchQueryBuilder.Type type) {
        this.type = type;
        return this;
    }

    /**
     * Sets the type of the text query.
     */
    public MultiMatchQueryBuilder type(Object type) {
        this.type = type == null ? null : Type.parse(type.toString().toLowerCase(Locale.ROOT));
        return this;
    }

    /**
     * Sets the operator to use when using a boolean query. Defaults to <tt>OR</tt>.
     */
    public MultiMatchQueryBuilder operator(MatchQueryBuilder.Operator operator) {
        this.operator = operator;
        return this;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping config for the field, or, if not
     * set, the default search analyzer.
     */
    public MultiMatchQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /**
     * Set the boost to apply to the query.
     */
    @Override
    public MultiMatchQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Set the phrase slop if evaluated to a phrase query type.
     */
    public MultiMatchQueryBuilder slop(int slop) {
        this.slop = slop;
        return this;
    }

    /**
     * Sets the fuzziness used when evaluated to a fuzzy query type. Defaults to "AUTO".
     */
    public MultiMatchQueryBuilder fuzziness(Object fuzziness) {
        this.fuzziness = Fuzziness.build(fuzziness);
        return this;
    }

    public MultiMatchQueryBuilder prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    /**
     * When using fuzzy or prefix type query, the number of term expansions to use. Defaults to unbounded
     * so its recommended to set it to a reasonable value for faster execution.
     */
    public MultiMatchQueryBuilder maxExpansions(int maxExpansions) {
        this.maxExpansions = maxExpansions;
        return this;
    }

    public MultiMatchQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    public MultiMatchQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    public MultiMatchQueryBuilder fuzzyRewrite(String fuzzyRewrite) {
        this.fuzzyRewrite = fuzzyRewrite;
        return this;
    }

    /**
     * @deprecated use a tieBreaker of 1.0f to disable "dis-max"
     * query or select the appropriate {@link Type}
     */
    @Deprecated
    public MultiMatchQueryBuilder useDisMax(boolean useDisMax) {
        this.useDisMax = useDisMax;
        return this;
    }

    /**
     * <p>Tie-Breaker for "best-match" disjunction queries (OR-Queries).
     * The tie breaker capability allows documents that match more than one query clause
     * (in this case on more than one field) to be scored better than documents that
     * match only the best of the fields, without confusing this with the better case of
     * two distinct matches in the multiple fields.</p>
     *
     * <p>A tie-breaker value of <tt>1.0</tt> is interpreted as a signal to score queries as
     * "most-match" queries where all matching query clauses are considered for scoring.</p>
     *
     * @see Type
     */
    public MultiMatchQueryBuilder tieBreaker(float tieBreaker) {
        this.tieBreaker = tieBreaker;
        return this;
    }

    /**
     * Sets whether format based failures will be ignored.
     */
    public MultiMatchQueryBuilder lenient(boolean lenient) {
        this.lenient = lenient;
        return this;
    }


    /**
     * Set a cutoff value in [0..1] (or absolute number >=1) representing the
     * maximum threshold of a terms document frequency to be considered a low
     * frequency term.
     */
    public MultiMatchQueryBuilder cutoffFrequency(float cutoff) {
        this.cutoffFrequency = cutoff;
        return this;
    }


    public MultiMatchQueryBuilder zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery zeroTermsQuery) {
        this.zeroTermsQuery = zeroTermsQuery;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public MultiMatchQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.field("query", text);
        builder.startArray("fields");
        for (String field : fields) {
            if (fieldsBoosts != null && fieldsBoosts.containsKey(field)) {
                field += "^" + fieldsBoosts.lget();
            }
            builder.value(field);
        }
        builder.endArray();

        if (type != null) {
            builder.field("type", type.toString().toLowerCase(Locale.ENGLISH));
        }
        if (operator != null) {
            builder.field("operator", operator.toString());
        }
        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }
        if (boost != null) {
            builder.field("boost", boost);
        }
        if (slop != null) {
            builder.field("slop", slop);
        }
        if (fuzziness != null) {
            fuzziness.toXContent(builder, params);
        }
        if (prefixLength != null) {
            builder.field("prefix_length", prefixLength);
        }
        if (maxExpansions != null) {
            builder.field("max_expansions", maxExpansions);
        }
        if (minimumShouldMatch != null) {
            builder.field("minimum_should_match", minimumShouldMatch);
        }
        if (rewrite != null) {
            builder.field("rewrite", rewrite);
        }
        if (fuzzyRewrite != null) {
            builder.field("fuzzy_rewrite", fuzzyRewrite);
        }

        if (useDisMax != null) {
            builder.field("use_dis_max", useDisMax);
        }

        if (tieBreaker != null) {
            builder.field("tie_breaker", tieBreaker);
        }

        if (lenient != null) {
            builder.field("lenient", lenient);
        }

        if (cutoffFrequency != null) {
            builder.field("cutoff_frequency", cutoffFrequency);
        }

        if (zeroTermsQuery != null) {
            builder.field("zero_terms_query", zeroTermsQuery.toString());
        }

        if (queryName != null) {
            builder.field("_name", queryName);
        }

        builder.endObject();
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Object value = null;
        float boost = 1.0f;
        Float tieBreaker = null;
        MultiMatchQueryBuilder.Type type = null;
        MultiMatchQuery multiMatchQuery = new MultiMatchQuery(parseContext);
        String minimumShouldMatch = null;
        Map<String, Float> fieldNameWithBoosts = Maps.newHashMap();
        String queryName = null;
        XContentParser.Token token;
        String currentFieldName = null;
        Boolean useDisMax = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ("fields".equals(currentFieldName)) {
                if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        extractFieldAndBoost(parseContext, parser, fieldNameWithBoosts);
                    }
                } else if (token.isValue()) {
                    extractFieldAndBoost(parseContext, parser, fieldNameWithBoosts);
                } else {
                    throw new QueryParsingException(parseContext.index(), "[" + NAME + "] query does not support [" + currentFieldName
                            + "]");
                }
            } else if (token.isValue()) {
                if ("query".equals(currentFieldName)) {
                    value = parser.objectText();
                } else if ("type".equals(currentFieldName)) {
                    type = MultiMatchQueryBuilder.Type.parse(parser.text(), parseContext.parseFlags());
                } else if ("analyzer".equals(currentFieldName)) {
                    String analyzer = parser.text();
                    if (parseContext.analysisService().analyzer(analyzer) == null) {
                        throw new QueryParsingException(parseContext.index(), "["+ NAME +"] analyzer [" + parser.text() + "] not found");
                    }
                    multiMatchQuery.setAnalyzer(analyzer);
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("slop".equals(currentFieldName) || "phrase_slop".equals(currentFieldName) || "phraseSlop".equals(currentFieldName)) {
                    multiMatchQuery.setPhraseSlop(parser.intValue());
                } else if (Fuzziness.FIELD.match(currentFieldName, parseContext.parseFlags())) {
                    multiMatchQuery.setFuzziness(Fuzziness.parse(parser));
                } else if ("prefix_length".equals(currentFieldName) || "prefixLength".equals(currentFieldName)) {
                    multiMatchQuery.setFuzzyPrefixLength(parser.intValue());
                } else if ("max_expansions".equals(currentFieldName) || "maxExpansions".equals(currentFieldName)) {
                    multiMatchQuery.setMaxExpansions(parser.intValue());
                } else if ("operator".equals(currentFieldName)) {
                    String op = parser.text();
                    if ("or".equalsIgnoreCase(op)) {
                        multiMatchQuery.setOccur(BooleanClause.Occur.SHOULD);
                    } else if ("and".equalsIgnoreCase(op)) {
                        multiMatchQuery.setOccur(BooleanClause.Occur.MUST);
                    } else {
                        throw new QueryParsingException(parseContext.index(), "text query requires operator to be either 'and' or 'or', not [" + op + "]");
                    }
                } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if ("rewrite".equals(currentFieldName)) {
                    multiMatchQuery.setRewriteMethod(QueryParsers.parseRewriteMethod(parser.textOrNull(), null));
                } else if ("fuzzy_rewrite".equals(currentFieldName) || "fuzzyRewrite".equals(currentFieldName)) {
                    multiMatchQuery.setFuzzyRewriteMethod(QueryParsers.parseRewriteMethod(parser.textOrNull(), null));
                } else if ("use_dis_max".equals(currentFieldName) || "useDisMax".equals(currentFieldName)) {
                    useDisMax = parser.booleanValue();
                } else if ("tie_breaker".equals(currentFieldName) || "tieBreaker".equals(currentFieldName)) {
                    multiMatchQuery.setTieBreaker(tieBreaker = parser.floatValue());
                }  else if ("cutoff_frequency".equals(currentFieldName)) {
                    multiMatchQuery.setCommonTermsCutoff(parser.floatValue());
                } else if ("lenient".equals(currentFieldName)) {
                    multiMatchQuery.setLenient(parser.booleanValue());
                } else if ("zero_terms_query".equals(currentFieldName)) {
                    String zeroTermsDocs = parser.text();
                    if ("none".equalsIgnoreCase(zeroTermsDocs)) {
                        multiMatchQuery.setZeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE);
                    } else if ("all".equalsIgnoreCase(zeroTermsDocs)) {
                        multiMatchQuery.setZeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL);
                    } else {
                        throw new QueryParsingException(parseContext.index(), "Unsupported zero_terms_docs value [" + zeroTermsDocs + "]");
                    }
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[match] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (value == null) {
            throw new QueryParsingException(parseContext.index(), "No text specified for multi_match query");
        }

        if (fieldNameWithBoosts.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "No fields specified for multi_match query");
        }
        if (type == null) {
            type = MultiMatchQueryBuilder.Type.BEST_FIELDS;
        }
        if (useDisMax != null) { // backwards foobar
            boolean typeUsesDismax = type.tieBreaker() != 1.0f;
            if (typeUsesDismax != useDisMax) {
                if (useDisMax && tieBreaker == null) {
                    multiMatchQuery.setTieBreaker(0.0f);
                } else {
                    multiMatchQuery.setTieBreaker(1.0f);
                }
            }
        }
        Query query = multiMatchQuery.parse(type, fieldNameWithBoosts, value, minimumShouldMatch);
        if (query == null) {
            return null;
        }

        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    private void extractFieldAndBoost(QueryParseContext parseContext, XContentParser parser, Map<String, Float> fieldNameWithBoosts) throws IOException {
        String fField = null;
        Float fBoost = null;
        char[] fieldText = parser.textCharacters();
        int end = parser.textOffset() + parser.textLength();
        for (int i = parser.textOffset(); i < end; i++) {
            if (fieldText[i] == '^') {
                int relativeLocation = i - parser.textOffset();
                fField = new String(fieldText, parser.textOffset(), relativeLocation);
                fBoost = Float.parseFloat(new String(fieldText, i + 1, parser.textLength() - relativeLocation - 1));
                break;
            }
        }
        if (fField == null) {
            fField = parser.text();
        }

        if (Regex.isSimpleMatchPattern(fField)) {
            for (String field : parseContext.mapperService().simpleMatchToIndexNames(fField)) {
                fieldNameWithBoosts.put(field, fBoost);
            }
        } else {
            fieldNameWithBoosts.put(fField, fBoost);
        }
    }
}
