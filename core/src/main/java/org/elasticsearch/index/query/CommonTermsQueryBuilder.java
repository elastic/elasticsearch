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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Objects;

/**
 * CommonTermsQuery query is a query that executes high-frequency terms in a
 * optional sub-query to prevent slow queries due to "common" terms like
 * stopwords. This query basically builds 2 queries off the {@code #add(Term)
 * added} terms where low-frequency terms are added to a required boolean clause
 * and high-frequency terms are added to an optional boolean clause. The
 * optional clause is only executed if the required "low-frequency' clause
 * matches.
 */
public class CommonTermsQueryBuilder extends AbstractQueryBuilder<CommonTermsQueryBuilder> {

    public static final String NAME = "common";

    public static final float DEFAULT_CUTOFF_FREQ = 0.01f;
    public static final Operator DEFAULT_HIGH_FREQ_OCCUR = Operator.OR;
    public static final Operator DEFAULT_LOW_FREQ_OCCUR = Operator.OR;
    public static final boolean DEFAULT_DISABLE_COORD = true;

    private static final ParseField CUTOFF_FREQUENCY_FIELD = new ParseField("cutoff_frequency");
    private static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");
    private static final ParseField LOW_FREQ_OPERATOR_FIELD = new ParseField("low_freq_operator");
    private static final ParseField HIGH_FREQ_OPERATOR_FIELD = new ParseField("high_freq_operator");
    private static final ParseField DISABLE_COORD_FIELD = new ParseField("disable_coord")
            .withAllDeprecated("disable_coord has been removed");
    private static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField HIGH_FREQ_FIELD = new ParseField("high_freq");
    private static final ParseField LOW_FREQ_FIELD = new ParseField("low_freq");

    private final String fieldName;

    private final Object text;

    private Operator highFreqOperator = DEFAULT_HIGH_FREQ_OCCUR;

    private Operator lowFreqOperator = DEFAULT_LOW_FREQ_OCCUR;

    private String analyzer = null;

    private String lowFreqMinimumShouldMatch = null;

    private String highFreqMinimumShouldMatch = null;

    private float cutoffFrequency = DEFAULT_CUTOFF_FREQ;

    /**
     * Constructs a new common terms query.
     */
    public CommonTermsQueryBuilder(String fieldName, Object text) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        if (text == null) {
            throw new IllegalArgumentException("text cannot be null");
        }
        this.fieldName = fieldName;
        this.text = text;
    }

    /**
     * Read from a stream.
     */
    public CommonTermsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        text = in.readGenericValue();
        highFreqOperator = Operator.readFromStream(in);
        lowFreqOperator = Operator.readFromStream(in);
        analyzer = in.readOptionalString();
        lowFreqMinimumShouldMatch = in.readOptionalString();
        highFreqMinimumShouldMatch = in.readOptionalString();
        if (in.getVersion().before(Version.V_6_0_0_alpha1)) {
            in.readBoolean(); // disable_coord
        }
        cutoffFrequency = in.readFloat();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        out.writeGenericValue(this.text);
        highFreqOperator.writeTo(out);
        lowFreqOperator.writeTo(out);
        out.writeOptionalString(analyzer);
        out.writeOptionalString(lowFreqMinimumShouldMatch);
        out.writeOptionalString(highFreqMinimumShouldMatch);
        if (out.getVersion().before(Version.V_6_0_0_alpha1)) {
            out.writeBoolean(true); // disable_coord
        }
        out.writeFloat(cutoffFrequency);
    }

    public String fieldName() {
        return this.fieldName;
    }

    public Object value() {
        return this.text;
    }

    /**
     * Sets the operator to use for terms with a high document frequency
     * (greater than or equal to {@link #cutoffFrequency(float)}. Defaults to
     * <tt>AND</tt>.
     */
    public CommonTermsQueryBuilder highFreqOperator(Operator operator) {
        this.highFreqOperator = (operator == null) ? DEFAULT_HIGH_FREQ_OCCUR : operator;
        return this;
    }

    public Operator highFreqOperator() {
        return highFreqOperator;
    }

    /**
     * Sets the operator to use for terms with a low document frequency (less
     * than {@link #cutoffFrequency(float)}. Defaults to <tt>AND</tt>.
     */
    public CommonTermsQueryBuilder lowFreqOperator(Operator operator) {
        this.lowFreqOperator = (operator == null) ? DEFAULT_LOW_FREQ_OCCUR : operator;
        return this;
    }

    public Operator lowFreqOperator() {
        return lowFreqOperator;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping
     * config for the field, or, if not set, the default search analyzer.
     */
    public CommonTermsQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public String analyzer() {
        return this.analyzer;
    }

    /**
     * Sets the cutoff document frequency for high / low frequent terms. A value
     * in [0..1] (or absolute number &gt;=1) representing the maximum threshold of
     * a terms document frequency to be considered a low frequency term.
     * Defaults to
     * <tt>{@value #DEFAULT_CUTOFF_FREQ}</tt>
     */
    public CommonTermsQueryBuilder cutoffFrequency(float cutoffFrequency) {
        this.cutoffFrequency = cutoffFrequency;
        return this;
    }

    public float cutoffFrequency() {
        return this.cutoffFrequency;
    }

    /**
     * Sets the minimum number of high frequent query terms that need to match in order to
     * produce a hit when there are no low frequent terms.
     */
    public CommonTermsQueryBuilder highFreqMinimumShouldMatch(String highFreqMinimumShouldMatch) {
        this.highFreqMinimumShouldMatch = highFreqMinimumShouldMatch;
        return this;
    }

    public String highFreqMinimumShouldMatch() {
        return this.highFreqMinimumShouldMatch;
    }

    /**
     * Sets the minimum number of low frequent query terms that need to match in order to
     * produce a hit.
     */
    public CommonTermsQueryBuilder lowFreqMinimumShouldMatch(String lowFreqMinimumShouldMatch) {
        this.lowFreqMinimumShouldMatch = lowFreqMinimumShouldMatch;
        return this;
    }

    public String lowFreqMinimumShouldMatch() {
        return this.lowFreqMinimumShouldMatch;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(QUERY_FIELD.getPreferredName(), text);
        builder.field(HIGH_FREQ_OPERATOR_FIELD.getPreferredName(), highFreqOperator.toString());
        builder.field(LOW_FREQ_OPERATOR_FIELD.getPreferredName(), lowFreqOperator.toString());
        if (analyzer != null) {
            builder.field(ANALYZER_FIELD.getPreferredName(), analyzer);
        }
        builder.field(CUTOFF_FREQUENCY_FIELD.getPreferredName(), cutoffFrequency);
        if (lowFreqMinimumShouldMatch != null || highFreqMinimumShouldMatch != null) {
            builder.startObject(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName());
            if (lowFreqMinimumShouldMatch != null) {
                builder.field(LOW_FREQ_FIELD.getPreferredName(), lowFreqMinimumShouldMatch);
            }
            if (highFreqMinimumShouldMatch != null) {
                builder.field(HIGH_FREQ_FIELD.getPreferredName(), highFreqMinimumShouldMatch);
            }
            builder.endObject();
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    public static CommonTermsQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        Object text = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String analyzer = null;
        String lowFreqMinimumShouldMatch = null;
        String highFreqMinimumShouldMatch = null;
        Operator highFreqOperator = CommonTermsQueryBuilder.DEFAULT_HIGH_FREQ_OCCUR;
        Operator lowFreqOperator = CommonTermsQueryBuilder.DEFAULT_LOW_FREQ_OCCUR;
        float cutoffFrequency = CommonTermsQueryBuilder.DEFAULT_CUTOFF_FREQ;
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
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (MINIMUM_SHOULD_MATCH_FIELD.match(currentFieldName)) {
                            String innerFieldName = null;
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    innerFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if (LOW_FREQ_FIELD.match(innerFieldName)) {
                                        lowFreqMinimumShouldMatch = parser.text();
                                    } else if (HIGH_FREQ_FIELD.match(innerFieldName)) {
                                        highFreqMinimumShouldMatch = parser.text();
                                    } else {
                                        throw new ParsingException(parser.getTokenLocation(), "[" + CommonTermsQueryBuilder.NAME +
                                                "] query does not support [" + innerFieldName
                                                + "] for [" + currentFieldName + "]");
                                    }
                                } else {
                                    throw new ParsingException(parser.getTokenLocation(), "[" + CommonTermsQueryBuilder.NAME +
                                            "] unexpected token type [" + token
                                            + "] after [" + innerFieldName + "]");
                                }
                            }
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "[" + CommonTermsQueryBuilder.NAME +
                                    "] query does not support [" + currentFieldName + "]");
                        }
                    } else if (token.isValue()) {
                        if (QUERY_FIELD.match(currentFieldName)) {
                            text = parser.objectText();
                        } else if (ANALYZER_FIELD.match(currentFieldName)) {
                            analyzer = parser.text();
                        } else if (DISABLE_COORD_FIELD.match(currentFieldName)) {
                            // ignore
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
                            boost = parser.floatValue();
                        } else if (HIGH_FREQ_OPERATOR_FIELD.match(currentFieldName)) {
                            highFreqOperator = Operator.fromString(parser.text());
                        } else if (LOW_FREQ_OPERATOR_FIELD.match(currentFieldName)) {
                            lowFreqOperator = Operator.fromString(parser.text());
                        } else if (MINIMUM_SHOULD_MATCH_FIELD.match(currentFieldName)) {
                            lowFreqMinimumShouldMatch = parser.text();
                        } else if (CUTOFF_FREQUENCY_FIELD.match(currentFieldName)) {
                            cutoffFrequency = parser.floatValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "[" + CommonTermsQueryBuilder.NAME +
                                    "] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                text = parser.objectText();
            }
        }

        return new CommonTermsQueryBuilder(fieldName, text)
                .lowFreqMinimumShouldMatch(lowFreqMinimumShouldMatch)
                .highFreqMinimumShouldMatch(highFreqMinimumShouldMatch)
                .analyzer(analyzer)
                .highFreqOperator(highFreqOperator)
                .lowFreqOperator(lowFreqOperator)
                .cutoffFrequency(cutoffFrequency)
                .boost(boost)
                .queryName(queryName);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        String field;
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType != null) {
            field = fieldType.name();
        } else {
            field = fieldName;
        }

        Analyzer analyzerObj;
        if (analyzer == null) {
            if (fieldType != null) {
                analyzerObj = context.getSearchAnalyzer(fieldType);
            } else {
                analyzerObj = context.getMapperService().searchAnalyzer();
            }
        } else {
            analyzerObj = context.getMapperService().getIndexAnalyzers().get(analyzer);
            if (analyzerObj == null) {
                throw new QueryShardException(context, "[common] analyzer [" + analyzer + "] not found");
            }
        }

        Occur highFreqOccur = highFreqOperator.toBooleanClauseOccur();
        Occur lowFreqOccur = lowFreqOperator.toBooleanClauseOccur();

        ExtendedCommonTermsQuery commonsQuery = new ExtendedCommonTermsQuery(highFreqOccur, lowFreqOccur,
                cutoffFrequency, fieldType);
        return parseQueryString(commonsQuery, text, field, analyzerObj, lowFreqMinimumShouldMatch, highFreqMinimumShouldMatch);
    }

    private static Query parseQueryString(ExtendedCommonTermsQuery query, Object queryString, String field, Analyzer analyzer,
                                         String lowFreqMinimumShouldMatch, String highFreqMinimumShouldMatch) throws IOException {
        // Logic similar to QueryParser#getFieldQuery
        try (TokenStream source = analyzer.tokenStream(field, queryString.toString())) {
            source.reset();
            CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);
            BytesRefBuilder builder = new BytesRefBuilder();
            while (source.incrementToken()) {
                // UTF-8
                builder.copyChars(termAtt);
                query.add(new Term(field, builder.toBytesRef()));
            }
        }

        query.setLowFreqMinimumNumberShouldMatch(lowFreqMinimumShouldMatch);
        query.setHighFreqMinimumNumberShouldMatch(highFreqMinimumShouldMatch);
        return query;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, text, highFreqOperator, lowFreqOperator, analyzer,
                lowFreqMinimumShouldMatch, highFreqMinimumShouldMatch, cutoffFrequency);
    }

    @Override
    protected boolean doEquals(CommonTermsQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
                Objects.equals(text, other.text) &&
                Objects.equals(highFreqOperator, other.highFreqOperator) &&
                Objects.equals(lowFreqOperator, other.lowFreqOperator) &&
                Objects.equals(analyzer, other.analyzer) &&
                Objects.equals(lowFreqMinimumShouldMatch, other.lowFreqMinimumShouldMatch) &&
                Objects.equals(highFreqMinimumShouldMatch, other.highFreqMinimumShouldMatch) &&
                Objects.equals(cutoffFrequency, other.cutoffFrequency);
    }
}
