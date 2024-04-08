/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.search.CoveringQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.TermsSetQueryScript;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class TermsSetQueryBuilder extends AbstractQueryBuilder<TermsSetQueryBuilder> {

    public static final String NAME = "terms_set";

    public static final TransportVersion MINIMUM_SHOULD_MATCH_ADDED_VERSION = TransportVersions.V_8_10_X;

    static final ParseField TERMS_FIELD = new ParseField("terms");
    static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match_field");
    static final ParseField MINIMUM_SHOULD_MATCH_SCRIPT = new ParseField("minimum_should_match_script");
    static final ParseField MINIMUM_SHOULD_MATCH = new ParseField("minimum_should_match");

    private final String fieldName;
    private final List<?> values;

    private String minimumShouldMatchField;
    private Script minimumShouldMatchScript;
    private String minimumShouldMatch;

    public TermsSetQueryBuilder(String fieldName, List<?> values) {
        this(fieldName, values, true);
    }

    private TermsSetQueryBuilder(String fieldName, List<?> values, boolean convert) {
        this.fieldName = Objects.requireNonNull(fieldName);
        Objects.requireNonNull(values);
        if (convert) {
            this.values = values.stream().map(AbstractQueryBuilder::maybeConvertToBytesRef).toList();
        } else {
            this.values = values;
        }
    }

    public TermsSetQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.values = (List<?>) in.readGenericValue();
        this.minimumShouldMatchField = in.readOptionalString();
        this.minimumShouldMatchScript = in.readOptionalWriteable(Script::new);
        if (in.getTransportVersion().onOrAfter(MINIMUM_SHOULD_MATCH_ADDED_VERSION)) {
            this.minimumShouldMatch = in.readOptionalString();
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(values);
        out.writeOptionalString(minimumShouldMatchField);
        out.writeOptionalWriteable(minimumShouldMatchScript);
        if (out.getTransportVersion().onOrAfter(MINIMUM_SHOULD_MATCH_ADDED_VERSION)) {
            out.writeOptionalString(minimumShouldMatch);
        }
    }

    // package protected for testing purpose
    String getFieldName() {
        return fieldName;
    }

    public List<?> getValues() {
        return values;
    }

    public String getMinimumShouldMatchField() {
        return minimumShouldMatchField;
    }

    public TermsSetQueryBuilder setMinimumShouldMatchField(String minimumShouldMatchField) {
        if (minimumShouldMatchScript != null || minimumShouldMatch != null) {
            throw new IllegalArgumentException(
                "A script or value has already been specified. Cannot specify both a field and a script or value"
            );
        }
        this.minimumShouldMatchField = minimumShouldMatchField;
        return this;
    }

    public Script getMinimumShouldMatchScript() {
        return minimumShouldMatchScript;
    }

    public TermsSetQueryBuilder setMinimumShouldMatchScript(Script minimumShouldMatchScript) {
        if (minimumShouldMatchField != null || minimumShouldMatch != null) {
            throw new IllegalArgumentException(
                "A field or value has already been specified. Cannot specify both a script and a field or value"
            );
        }
        this.minimumShouldMatchScript = minimumShouldMatchScript;
        return this;
    }

    public String getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    public TermsSetQueryBuilder setMinimumShouldMatch(String minimumShouldMatch) {
        if (minimumShouldMatchField != null || minimumShouldMatchScript != null) {
            throw new IllegalArgumentException(
                "A field or script has already been specified. Cannot specify both a value and a script or field"
            );
        }
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    @Override
    protected boolean doEquals(TermsSetQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(values, other.values)
            && Objects.equals(minimumShouldMatchField, other.minimumShouldMatchField)
            && Objects.equals(minimumShouldMatchScript, other.minimumShouldMatchScript)
            && Objects.equals(minimumShouldMatch, other.minimumShouldMatch);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, values, minimumShouldMatchField, minimumShouldMatchScript, minimumShouldMatch);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(TERMS_FIELD.getPreferredName(), convertBack(values));
        if (minimumShouldMatchField != null) {
            builder.field(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), minimumShouldMatchField);
        }
        if (minimumShouldMatchScript != null) {
            builder.field(MINIMUM_SHOULD_MATCH_SCRIPT.getPreferredName(), minimumShouldMatchScript);
        }
        if (minimumShouldMatch != null) {
            builder.field(MINIMUM_SHOULD_MATCH.getPreferredName(), minimumShouldMatch);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    public static TermsSetQueryBuilder fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unknown token [" + token + "]");
        }
        String currentFieldName = parser.currentName();
        String fieldName = currentFieldName;

        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unknown token [" + token + "]");
        }

        List<Object> values = new ArrayList<>();
        String minimumShouldMatchField = null;
        String minimumShouldMatch = null;
        Script minimumShouldMatchScript = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (TERMS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    values = TermsQueryBuilder.parseValues(parser);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (MINIMUM_SHOULD_MATCH_SCRIPT.match(currentFieldName, parser.getDeprecationHandler())) {
                    minimumShouldMatchScript = Script.parse(parser);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else if (token.isValue()) {
                if (MINIMUM_SHOULD_MATCH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    minimumShouldMatchField = parser.text();
                } else if (MINIMUM_SHOULD_MATCH.match(currentFieldName, parser.getDeprecationHandler())) {
                    minimumShouldMatch = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }

        token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unknown token [" + token + "]");
        }

        TermsSetQueryBuilder queryBuilder = new TermsSetQueryBuilder(fieldName, values, false).queryName(queryName).boost(boost);
        if (minimumShouldMatchField != null) {
            queryBuilder.setMinimumShouldMatchField(minimumShouldMatchField);
        }
        if (minimumShouldMatch != null) {
            queryBuilder.setMinimumShouldMatch(minimumShouldMatch);
        }
        if (minimumShouldMatchScript != null) {
            queryBuilder.setMinimumShouldMatchScript(minimumShouldMatchScript);
        }
        return queryBuilder;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) {
        if (values.isEmpty()) {
            return Queries.newMatchNoDocsQuery("No terms supplied for \"" + getName() + "\" query.");
        }
        // Fail before we attempt to create the term queries:
        if (values.size() > BooleanQuery.getMaxClauseCount()) {
            throw new BooleanQuery.TooManyClauses();
        }

        List<Query> queries = createTermQueries(context);
        LongValuesSource longValuesSource = createValuesSource(context);
        return new CoveringQuery(queries, longValuesSource);
    }

    /**
     * Visible only for testing purposes.
     */
    List<Query> createTermQueries(SearchExecutionContext context) {
        final MappedFieldType fieldType = context.getFieldType(fieldName);
        final List<Query> queries = new ArrayList<>(values.size());
        for (Object value : values) {
            if (fieldType != null) {
                queries.add(fieldType.termQuery(value, context));
            } else {
                queries.add(new TermQuery(new Term(fieldName, BytesRefs.toBytesRef(value))));
            }
        }
        return queries;
    }

    private LongValuesSource createValuesSource(SearchExecutionContext context) {
        LongValuesSource longValuesSource;
        if (minimumShouldMatch != null) {
            longValuesSource = LongValuesSource.constant(Queries.calculateMinShouldMatch(values.size(), minimumShouldMatch));
        } else if (minimumShouldMatchField != null) {
            MappedFieldType msmFieldType = context.getFieldType(minimumShouldMatchField);
            if (msmFieldType == null) {
                throw new QueryShardException(context, "failed to find minimum_should_match field [" + minimumShouldMatchField + "]");
            }

            IndexNumericFieldData fieldData = context.getForField(msmFieldType, MappedFieldType.FielddataOperation.SEARCH);
            longValuesSource = new FieldValuesSource(fieldData);
        } else if (minimumShouldMatchScript != null) {
            TermsSetQueryScript.Factory factory = context.compile(minimumShouldMatchScript, TermsSetQueryScript.CONTEXT);
            Map<String, Object> params = new HashMap<>();
            params.putAll(minimumShouldMatchScript.getParams());
            params.put("num_terms", values.size());
            longValuesSource = new ScriptLongValueSource(minimumShouldMatchScript, factory.newFactory(params, context.lookup()));
        } else {
            throw new IllegalStateException("No minimum should match has been specified");
        }
        return longValuesSource;
    }

    static final class ScriptLongValueSource extends LongValuesSource {

        private final Script script;
        private final TermsSetQueryScript.LeafFactory leafFactory;

        ScriptLongValueSource(Script script, TermsSetQueryScript.LeafFactory leafFactory) {
            this.script = script;
            this.leafFactory = leafFactory;
        }

        @Override
        public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
            TermsSetQueryScript script = leafFactory.newInstance(ctx);
            return new LongValues() {
                @Override
                public long longValue() throws IOException {
                    return script.runAsLong();
                }

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    script.setDocument(doc);
                    return script.execute() != null;
                }
            };
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public int hashCode() {
            int h = getClass().hashCode();
            h = 31 * h + script.hashCode();
            return h;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ScriptLongValueSource that = (ScriptLongValueSource) obj;
            return Objects.equals(script, that.script);
        }

        @Override
        public String toString() {
            return "script(" + script.toString() + ")";
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            // TODO: Change this to true when we can assume that scripts are pure functions
            // ie. the return value is always the same given the same conditions and may not
            // depend on the current timestamp, other documents, etc.
            return false;
        }

        @Override
        public LongValuesSource rewrite(IndexSearcher searcher) throws IOException {
            return this;
        }

    }

    /**
     * Convert the internal {@link List} of values back to a user-friendly list.
     */
    private static List<Object> convertBack(List<?> list) {
        return new AbstractList<Object>() {
            @Override
            public int size() {
                return list.size();
            }

            @Override
            public Object get(int index) {
                return maybeConvertToString(list.get(index));
            }
        };
    }

    // Forked from LongValuesSource.FieldValuesSource and changed getValues() method to always use sorted numeric
    // doc values, because that is what is being used in NumberFieldMapper.
    static class FieldValuesSource extends LongValuesSource {

        private final String fieldName;
        private final IndexNumericFieldData fieldData;

        FieldValuesSource(IndexNumericFieldData fieldData) {
            this.fieldData = fieldData;
            this.fieldName = fieldData.getFieldName();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FieldValuesSource that = (FieldValuesSource) o;
            return Objects.equals(fieldName, that.fieldName);
        }

        @Override
        public String toString() {
            return "long(" + fieldName + ")";
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName);
        }

        @Override
        public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
            SortedNumericDocValues values = fieldData.load(ctx).getLongValues();
            return new LongValues() {

                long current = -1;

                @Override
                public long longValue() throws IOException {
                    return current;
                }

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    boolean hasValue = values.advanceExact(doc);
                    if (hasValue) {
                        assert values.docValueCount() == 1;
                        current = values.nextValue();
                        return true;
                    } else {
                        return false;
                    }
                }
            };
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }

        @Override
        public LongValuesSource rewrite(IndexSearcher searcher) throws IOException {
            return this;
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
