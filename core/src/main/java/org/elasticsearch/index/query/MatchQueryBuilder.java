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

import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.search.MatchQuery;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Match query is a query that analyzes the text and constructs a query as the result of the analysis. It
 * can construct different queries based on the type provided.
 */
public class MatchQueryBuilder extends AbstractQueryBuilder<MatchQueryBuilder> {

    /** The default name for the match query */
    public static final String NAME = "match";

    /** The default mode terms are combined in a match query */
    public static final Operator DEFAULT_OPERATOR = Operator.OR;

    /** The default mode match query type */
    public static final MatchQuery.Type DEFAULT_TYPE = MatchQuery.Type.BOOLEAN;

    private final String fieldName;

    private final Object value;

    private MatchQuery.Type type = DEFAULT_TYPE;

    private Operator operator = DEFAULT_OPERATOR;

    private String analyzer;

    private int slop = MatchQuery.DEFAULT_PHRASE_SLOP;

    private Fuzziness fuzziness = null;

    private int prefixLength = FuzzyQuery.defaultPrefixLength;

    private int  maxExpansions = FuzzyQuery.defaultMaxExpansions;

    private boolean fuzzyTranspositions = FuzzyQuery.defaultTranspositions;

    private String minimumShouldMatch;

    private String fuzzyRewrite = null;

    private boolean lenient = MatchQuery.DEFAULT_LENIENCY;

    private MatchQuery.ZeroTermsQuery zeroTermsQuery = MatchQuery.DEFAULT_ZERO_TERMS_QUERY;

    private Float cutoffFrequency = null;

    static final MatchQueryBuilder PROTOTYPE = new MatchQueryBuilder("","");

    /**
     * Constructs a new match query.
     */
    public MatchQueryBuilder(String fieldName, Object value) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires fieldName");
        }
        if (value == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query value");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    /** Returns the field name used in this query. */
    public String fieldName() {
        return this.fieldName;
    }

    /** Returns the value used in this query. */
    public Object value() {
        return this.value;
    }

    /** Sets the type of the text query. */
    public MatchQueryBuilder type(MatchQuery.Type type) {
        if (type == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires type to be non-null");
        }
        this.type = type;
        return this;
    }

    /** Get the type of the query. */
    public MatchQuery.Type type() {
        return this.type;
    }

    /** Sets the operator to use when using a boolean query. Defaults to <tt>OR</tt>. */
    public MatchQueryBuilder operator(Operator operator) {
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

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping config for the field, or, if not
     * set, the default search analyzer.
     */
    public MatchQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /** Get the analyzer to use, if previously set, otherwise <tt>null</tt> */
    public String analyzer() {
        return this.analyzer;
    }

    /** Sets a slop factor for phrase queries */
    public MatchQueryBuilder slop(int slop) {
        if (slop < 0 ) {
            throw new IllegalArgumentException("No negative slop allowed.");
        }
        this.slop = slop;
        return this;
    }

    /** Get the slop factor for phrase queries. */
    public int slop() {
        return this.slop;
    }

    /** Sets the fuzziness used when evaluated to a fuzzy query type. Defaults to "AUTO". */
    public MatchQueryBuilder fuzziness(Object fuzziness) {
        this.fuzziness = Fuzziness.build(fuzziness);
        return this;
    }

    /**  Gets the fuzziness used when evaluated to a fuzzy query type. */
    public Fuzziness fuzziness() {
        return this.fuzziness;
    }

    /**
     * Sets the length of a length of common (non-fuzzy) prefix for fuzzy match queries
     * @param prefixLength non-negative length of prefix
     * @throws IllegalArgumentException in case the prefix is negative
     */
    public MatchQueryBuilder prefixLength(int prefixLength) {
        if (prefixLength < 0 ) {
            throw new IllegalArgumentException("No negative prefix length allowed.");
        }
        this.prefixLength = prefixLength;
        return this;
    }

    /**
     * Gets the length of a length of common (non-fuzzy) prefix for fuzzy match queries
     */
    public int prefixLength() {
        return this.prefixLength;
    }

    /**
     * When using fuzzy or prefix type query, the number of term expansions to use.
     */
    public MatchQueryBuilder maxExpansions(int maxExpansions) {
        if (maxExpansions < 0 ) {
            throw new IllegalArgumentException("No negative maxExpansions allowed.");
        }
        this.maxExpansions = maxExpansions;
        return this;
    }

    /**
     * Get the (optional) number of term expansions when using fuzzy or prefix type query.
     */
    public int maxExpansions() {
        return this.maxExpansions;
    }

    /**
     * Set a cutoff value in [0..1] (or absolute number &gt;=1) representing the
     * maximum threshold of a terms document frequency to be considered a low
     * frequency term.
     */
    public MatchQueryBuilder cutoffFrequency(float cutoff) {
        this.cutoffFrequency = cutoff;
        return this;
    }

    /** Gets the optional cutoff value, can be <tt>null</tt> if not set previously */
    public Float cutoffFrequency() {
        return this.cutoffFrequency;
    }

    /** Sets optional minimumShouldMatch value to apply to the query */
    public MatchQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /** Gets the minimumShouldMatch value */
    public String minimumShouldMatch() {
        return this.minimumShouldMatch;
    }

    /** Sets the fuzzy_rewrite parameter controlling how the fuzzy query will get rewritten */
    public MatchQueryBuilder fuzzyRewrite(String fuzzyRewrite) {
        this.fuzzyRewrite = fuzzyRewrite;
        return this;
    }

    /**
     * Get the fuzzy_rewrite parameter
     * @see #fuzzyRewrite(String)
     */
    public String fuzzyRewrite() {
        return this.fuzzyRewrite;
    }

    /**
     * Sets whether transpositions are supported in fuzzy queries.<p>
     * The default metric used by fuzzy queries to determine a match is the Damerau-Levenshtein
     * distance formula which supports transpositions. Setting transposition to false will
     * switch to classic Levenshtein distance.<br>
     * If not set, Damerau-Levenshtein distance metric will be used.
     */
    public MatchQueryBuilder fuzzyTranspositions(boolean fuzzyTranspositions) {
        this.fuzzyTranspositions = fuzzyTranspositions;
        return this;
    }

    /** Gets the fuzzy query transposition setting. */
    public boolean fuzzyTranspositions() {
        return this.fuzzyTranspositions;
    }

    /**
     * Sets whether format based failures will be ignored.
     * @deprecated use #lenient() instead
     */
    @Deprecated
    public MatchQueryBuilder setLenient(boolean lenient) {
        return lenient(lenient);
    }

    /**
     * Sets whether format based failures will be ignored.
     */
    public MatchQueryBuilder lenient(boolean lenient) {
        this.lenient = lenient;
        return this;
    }

    /**
     * Gets leniency setting that controls if format based failures will be ignored.
     */
    public boolean lenient() {
        return this.lenient;
    }

    /**
     * Sets query to use in case no query terms are available, e.g. after analysis removed them.
     * Defaults to {@link MatchQuery.ZeroTermsQuery#NONE}, but can be set to
     * {@link MatchQuery.ZeroTermsQuery#ALL} instead.
     */
    public MatchQueryBuilder zeroTermsQuery(MatchQuery.ZeroTermsQuery zeroTermsQuery) {
        if (zeroTermsQuery == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires zeroTermsQuery to be non-null");
        }
        this.zeroTermsQuery = zeroTermsQuery;
        return this;
    }

    /**
     * Returns the setting for handling zero terms queries.
     */
    public MatchQuery.ZeroTermsQuery zeroTermsQuery() {
        return this.zeroTermsQuery;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);

        builder.field(MatchQueryParser.QUERY_FIELD.getPreferredName(), value);
        builder.field(MatchQueryParser.TYPE_FIELD.getPreferredName(), type.toString().toLowerCase(Locale.ENGLISH));
        builder.field(MatchQueryParser.OPERATOR_FIELD.getPreferredName(), operator.toString());
        if (analyzer != null) {
            builder.field(MatchQueryParser.ANALYZER_FIELD.getPreferredName(), analyzer);
        }
        builder.field(MatchQueryParser.SLOP_FIELD.getPreferredName(), slop);
        if (fuzziness != null) {
            fuzziness.toXContent(builder, params);
        }
        builder.field(MatchQueryParser.PREFIX_LENGTH_FIELD.getPreferredName(), prefixLength);
        builder.field(MatchQueryParser.MAX_EXPANSIONS_FIELD.getPreferredName(), maxExpansions);
        if (minimumShouldMatch != null) {
            builder.field(MatchQueryParser.MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), minimumShouldMatch);
        }
        if (fuzzyRewrite != null) {
            builder.field(MatchQueryParser.FUZZY_REWRITE_FIELD.getPreferredName(), fuzzyRewrite);
        }
        // LUCENE 4 UPGRADE we need to document this & test this
        builder.field(MatchQueryParser.FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), fuzzyTranspositions);
        builder.field(MatchQueryParser.LENIENT_FIELD.getPreferredName(), lenient);
        builder.field(MatchQueryParser.ZERO_TERMS_QUERY_FIELD.getPreferredName(), zeroTermsQuery.toString());
        if (cutoffFrequency != null) {
            builder.field(MatchQueryParser.CUTOFF_FREQUENCY_FIELD.getPreferredName(), cutoffFrequency);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // validate context specific fields
        if (analyzer != null && context.getAnalysisService().analyzer(analyzer) == null) {
            throw new QueryShardException(context, "[match] analyzer [" + analyzer + "] not found");
        }

        MatchQuery matchQuery = new MatchQuery(context);
        matchQuery.setOccur(operator.toBooleanClauseOccur());
        matchQuery.setAnalyzer(analyzer);
        matchQuery.setPhraseSlop(slop);
        matchQuery.setFuzziness(fuzziness);
        matchQuery.setFuzzyPrefixLength(prefixLength);
        matchQuery.setMaxExpansions(maxExpansions);
        matchQuery.setTranspositions(fuzzyTranspositions);
        matchQuery.setFuzzyRewriteMethod(QueryParsers.parseRewriteMethod(context.parseFieldMatcher(), fuzzyRewrite, null));
        matchQuery.setLenient(lenient);
        matchQuery.setCommonTermsCutoff(cutoffFrequency);
        matchQuery.setZeroTermsQuery(zeroTermsQuery);

        Query query = matchQuery.parse(type, fieldName, value);
        if (query == null) {
            return null;
        }

        if (query instanceof BooleanQuery) {
            query = Queries.applyMinimumShouldMatch((BooleanQuery) query, minimumShouldMatch);
        } else if (query instanceof ExtendedCommonTermsQuery) {
            ((ExtendedCommonTermsQuery)query).setLowFreqMinimumNumberShouldMatch(minimumShouldMatch);
        }
        return query;
    }

    @Override
    protected boolean doEquals(MatchQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
               Objects.equals(value, other.value) &&
               Objects.equals(type, other.type) &&
               Objects.equals(operator, other.operator) &&
               Objects.equals(analyzer, other.analyzer) &&
               Objects.equals(slop, other.slop) &&
               Objects.equals(fuzziness, other.fuzziness) &&
               Objects.equals(prefixLength, other.prefixLength) &&
               Objects.equals(maxExpansions, other.maxExpansions) &&
               Objects.equals(minimumShouldMatch, other.minimumShouldMatch) &&
               Objects.equals(fuzzyRewrite, other.fuzzyRewrite) &&
               Objects.equals(lenient, other.lenient) &&
               Objects.equals(fuzzyTranspositions, other.fuzzyTranspositions) &&
               Objects.equals(zeroTermsQuery, other.zeroTermsQuery) &&
               Objects.equals(cutoffFrequency, other.cutoffFrequency);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value, type, operator, analyzer, slop,
                fuzziness, prefixLength, maxExpansions, minimumShouldMatch,
                fuzzyRewrite, lenient, fuzzyTranspositions, zeroTermsQuery, cutoffFrequency);
    }

    @Override
    protected MatchQueryBuilder doReadFrom(StreamInput in) throws IOException {
        MatchQueryBuilder matchQuery = new MatchQueryBuilder(in.readString(), in.readGenericValue());
        matchQuery.type = MatchQuery.Type.readTypeFrom(in);
        matchQuery.operator = Operator.readOperatorFrom(in);
        matchQuery.slop = in.readVInt();
        matchQuery.prefixLength = in.readVInt();
        matchQuery.maxExpansions = in.readVInt();
        matchQuery.fuzzyTranspositions = in.readBoolean();
        matchQuery.lenient = in.readBoolean();
        matchQuery.zeroTermsQuery = MatchQuery.ZeroTermsQuery.readZeroTermsQueryFrom(in);
        // optional fields
        matchQuery.analyzer = in.readOptionalString();
        matchQuery.minimumShouldMatch = in.readOptionalString();
        matchQuery.fuzzyRewrite = in.readOptionalString();
        if (in.readBoolean()) {
            matchQuery.fuzziness = Fuzziness.readFuzzinessFrom(in);
        }
        if (in.readBoolean()) {
            matchQuery.cutoffFrequency = in.readFloat();
        }
        return matchQuery;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(value);
        type.writeTo(out);
        operator.writeTo(out);
        out.writeVInt(slop);
        out.writeVInt(prefixLength);
        out.writeVInt(maxExpansions);
        out.writeBoolean(fuzzyTranspositions);
        out.writeBoolean(lenient);
        zeroTermsQuery.writeTo(out);
        // optional fields
        out.writeOptionalString(analyzer);
        out.writeOptionalString(minimumShouldMatch);
        out.writeOptionalString(fuzzyRewrite);
        if (fuzziness == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            fuzziness.writeTo(out);
        }
        if (cutoffFrequency == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeFloat(cutoffFrequency);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
