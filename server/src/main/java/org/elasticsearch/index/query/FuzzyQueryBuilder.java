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

import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;
import java.util.Objects;

/**
 * A Query that does fuzzy matching for a specific value.
 */
public class FuzzyQueryBuilder extends AbstractQueryBuilder<FuzzyQueryBuilder> implements MultiTermQueryBuilder {
    public static final String NAME = "fuzzy";

    /** Default maximum edit distance. Defaults to AUTO. */
    public static final Fuzziness DEFAULT_FUZZINESS = Fuzziness.AUTO;

    /** Default number of initial characters which will not be “fuzzified”. Defaults to 0. */
    public static final int DEFAULT_PREFIX_LENGTH = FuzzyQuery.defaultPrefixLength;

    /** Default maximum number of terms that the fuzzy query will expand to. Defaults to 50. */
    public static final int DEFAULT_MAX_EXPANSIONS = FuzzyQuery.defaultMaxExpansions;

    /** Default as to whether transpositions should be treated as a primitive edit operation,
     * instead of classic Levenshtein algorithm. Defaults to true. */
    public static final boolean DEFAULT_TRANSPOSITIONS = FuzzyQuery.defaultTranspositions;

    private static final ParseField TERM_FIELD = new ParseField("term");
    private static final ParseField VALUE_FIELD = new ParseField("value");
    private static final ParseField PREFIX_LENGTH_FIELD = new ParseField("prefix_length");
    private static final ParseField MAX_EXPANSIONS_FIELD = new ParseField("max_expansions");
    private static final ParseField TRANSPOSITIONS_FIELD = new ParseField("transpositions");
    private static final ParseField REWRITE_FIELD = new ParseField("rewrite");

    private final String fieldName;

    private final Object value;

    private Fuzziness fuzziness = DEFAULT_FUZZINESS;

    private int prefixLength = DEFAULT_PREFIX_LENGTH;

    private int maxExpansions = DEFAULT_MAX_EXPANSIONS;

    private boolean transpositions = DEFAULT_TRANSPOSITIONS;

    private String rewrite;

    /**
     * Constructs a new fuzzy query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the text
     */
    public FuzzyQueryBuilder(String fieldName, String value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new fuzzy query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the text
     */
    public FuzzyQueryBuilder(String fieldName, int value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new fuzzy query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the text
     */
    public FuzzyQueryBuilder(String fieldName, long value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new fuzzy query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the text
     */
    public FuzzyQueryBuilder(String fieldName, float value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new fuzzy query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the text
     */
    public FuzzyQueryBuilder(String fieldName, double value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new fuzzy query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the text
     */
    public FuzzyQueryBuilder(String fieldName, boolean value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new fuzzy query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public FuzzyQueryBuilder(String fieldName, Object value) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name cannot be null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("query value cannot be null");
        }
        this.fieldName = fieldName;
        this.value = maybeConvertToBytesRef(value);
    }

    /**
     * Read from a stream.
     */
    public FuzzyQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        value = in.readGenericValue();
        fuzziness = new Fuzziness(in);
        prefixLength = in.readVInt();
        maxExpansions = in.readVInt();
        transpositions = in.readBoolean();
        rewrite = in.readOptionalString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        out.writeGenericValue(this.value);
        this.fuzziness.writeTo(out);
        out.writeVInt(this.prefixLength);
        out.writeVInt(this.maxExpansions);
        out.writeBoolean(this.transpositions);
        out.writeOptionalString(this.rewrite);
    }

    @Override
    public String fieldName() {
        return this.fieldName;
    }

    public Object value() {
        return maybeConvertToString(this.value);
    }

    public FuzzyQueryBuilder fuzziness(Fuzziness fuzziness) {
        this.fuzziness = (fuzziness == null) ? DEFAULT_FUZZINESS : fuzziness;
        return this;
    }

    public Fuzziness fuzziness() {
        return this.fuzziness;
    }

    public FuzzyQueryBuilder prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    public int prefixLength() {
        return this.prefixLength;
    }

    public FuzzyQueryBuilder maxExpansions(int maxExpansions) {
        this.maxExpansions = maxExpansions;
        return this;
    }

    public int maxExpansions() {
        return this.maxExpansions;
    }

    public FuzzyQueryBuilder transpositions(boolean transpositions) {
      this.transpositions = transpositions;
      return this;
    }

    public boolean transpositions() {
        return this.transpositions;
    }

    public FuzzyQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    public String rewrite() {
        return this.rewrite;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(VALUE_FIELD.getPreferredName(), maybeConvertToString(this.value));
        fuzziness.toXContent(builder, params);
        builder.field(PREFIX_LENGTH_FIELD.getPreferredName(), prefixLength);
        builder.field(MAX_EXPANSIONS_FIELD.getPreferredName(), maxExpansions);
        builder.field(TRANSPOSITIONS_FIELD.getPreferredName(), transpositions);
        if (rewrite != null) {
            builder.field(REWRITE_FIELD.getPreferredName(), rewrite);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    public static FuzzyQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        Object value = null;
        Fuzziness fuzziness = FuzzyQueryBuilder.DEFAULT_FUZZINESS;
        int prefixLength = FuzzyQueryBuilder.DEFAULT_PREFIX_LENGTH;
        int maxExpansions = FuzzyQueryBuilder.DEFAULT_MAX_EXPANSIONS;
        boolean transpositions = FuzzyQueryBuilder.DEFAULT_TRANSPOSITIONS;
        String rewrite = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String currentFieldName = null;
        XContentParser.Token token;
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
                        if (TERM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = maybeConvertToBytesRef(parser.objectBytes());
                        } else if (VALUE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = maybeConvertToBytesRef(parser.objectBytes());
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (Fuzziness.FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            fuzziness = Fuzziness.parse(parser);
                        } else if (PREFIX_LENGTH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            prefixLength = parser.intValue();
                        } else if (MAX_EXPANSIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            maxExpansions = parser.intValue();
                        } else if (TRANSPOSITIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            transpositions = parser.booleanValue();
                        } else if (REWRITE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            rewrite = parser.textOrNull();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "[fuzzy] query does not support [" + currentFieldName + "]");
                        }
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                                "[" + NAME + "] unexpected token [" + token + "] after [" + currentFieldName + "]");
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                value = maybeConvertToBytesRef(parser.objectBytes());
            }
        }
        return new FuzzyQueryBuilder(fieldName, value)
                .fuzziness(fuzziness)
                .prefixLength(prefixLength)
                .maxExpansions(maxExpansions)
                .transpositions(transpositions)
                .rewrite(rewrite)
                .boost(boost)
                .queryName(queryName);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryShardContext context = queryRewriteContext.convertToShardContext();
        if (context != null) {
            MappedFieldType fieldType = context.fieldMapper(fieldName);
            if (fieldType == null) {
                return new MatchNoneQueryBuilder();
            }
        }
        return super.doRewrite(context);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new IllegalStateException("Rewrite first");
        }
        String rewrite = this.rewrite;
        Query query = fieldType.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, context);
        if (query instanceof MultiTermQuery) {
            MultiTermQuery.RewriteMethod rewriteMethod = QueryParsers.parseRewriteMethod(rewrite, null,
                LoggingDeprecationHandler.INSTANCE);
            QueryParsers.setRewriteMethod((MultiTermQuery) query, rewriteMethod);
        }
        return query;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value, fuzziness, prefixLength, maxExpansions, transpositions, rewrite);
    }

    @Override
    protected boolean doEquals(FuzzyQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
                Objects.equals(value, other.value) &&
                Objects.equals(fuzziness, other.fuzziness) &&
                Objects.equals(prefixLength, other.prefixLength) &&
                Objects.equals(maxExpansions, other.maxExpansions) &&
                Objects.equals(transpositions, other.transpositions) &&
                Objects.equals(rewrite, other.rewrite);
    }
}
