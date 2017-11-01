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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CoveringQuery;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class TermsSetQueryBuilder extends AbstractQueryBuilder<TermsSetQueryBuilder> {

    public static final String NAME = "terms_set";

    static final ParseField TERMS_FIELD = new ParseField("terms");
    static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match_field");
    static final ParseField MINIMUM_SHOULD_MATCH_SCRIPT = new ParseField("minimum_should_match_script");

    private final String fieldName;
    private final List<?> values;

    private String minimumShouldMatchField;
    private Script minimumShouldMatchScript;

    public TermsSetQueryBuilder(String fieldName, List<?> values) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.values = TermsQueryBuilder.convert(Objects.requireNonNull(values));
    }

    public TermsSetQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.values = (List<?>) in.readGenericValue();
        this.minimumShouldMatchField = in.readOptionalString();
        this.minimumShouldMatchScript = in.readOptionalWriteable(Script::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(values);
        out.writeOptionalString(minimumShouldMatchField);
        out.writeOptionalWriteable(minimumShouldMatchScript);
    }

    public List<?> getValues() {
        return values;
    }

    public String getMinimumShouldMatchField() {
        return minimumShouldMatchField;
    }

    public TermsSetQueryBuilder setMinimumShouldMatchField(String minimumShouldMatchField) {
        if (minimumShouldMatchScript != null) {
            throw new IllegalArgumentException("A script has already been specified. Cannot specify both a field and script");
        }
        this.minimumShouldMatchField = minimumShouldMatchField;
        return this;
    }

    public Script getMinimumShouldMatchScript() {
        return minimumShouldMatchScript;
    }

    public TermsSetQueryBuilder setMinimumShouldMatchScript(Script minimumShouldMatchScript) {
        if (minimumShouldMatchField != null) {
            throw new IllegalArgumentException("A field has already been specified. Cannot specify both a field and script");
        }
        this.minimumShouldMatchScript = minimumShouldMatchScript;
        return this;
    }

    @Override
    protected boolean doEquals(TermsSetQueryBuilder other) {
        return Objects.equals(fieldName, this.fieldName) && Objects.equals(values, this.values) &&
                Objects.equals(minimumShouldMatchField, this.minimumShouldMatchField) &&
                Objects.equals(minimumShouldMatchScript, this.minimumShouldMatchScript);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, values, minimumShouldMatchField, minimumShouldMatchScript);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(TERMS_FIELD.getPreferredName(), TermsQueryBuilder.convertBack(values));
        if (minimumShouldMatchField != null) {
            builder.field(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), minimumShouldMatchField);
        }
        if (minimumShouldMatchScript != null) {
            builder.field(MINIMUM_SHOULD_MATCH_SCRIPT.getPreferredName(), minimumShouldMatchScript);
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
        Script minimumShouldMatchScript = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (TERMS_FIELD.match(currentFieldName)) {
                    values = TermsQueryBuilder.parseValues(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] query does not support ["
                            + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (MINIMUM_SHOULD_MATCH_SCRIPT.match(currentFieldName)) {
                    minimumShouldMatchScript = Script.parse(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] query does not support ["
                            + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (MINIMUM_SHOULD_MATCH_FIELD.match(currentFieldName)) {
                    minimumShouldMatchField = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] query does not support ["
                            + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unknown token [" + token +
                        "] after [" + currentFieldName + "]");
            }
        }

        token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unknown token [" + token + "]");
        }

        TermsSetQueryBuilder queryBuilder = new TermsSetQueryBuilder(fieldName, values)
                .queryName(queryName).boost(boost);
        if (minimumShouldMatchField != null) {
            queryBuilder.setMinimumShouldMatchField(minimumShouldMatchField);
        }
        if (minimumShouldMatchScript != null) {
            queryBuilder.setMinimumShouldMatchScript(minimumShouldMatchScript);
        }
        return queryBuilder;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (values.isEmpty()) {
            return Queries.newMatchNoDocsQuery("No terms supplied for \"" + getName() + "\" query.");
        }
        // Fail before we attempt to create the term queries:
        if (values.size() > BooleanQuery.getMaxClauseCount()) {
            throw new BooleanQuery.TooManyClauses();
        }

        final MappedFieldType fieldType = context.fieldMapper(fieldName);
        final List<Query> queries = new ArrayList<>(values.size());
        for (Object value : values) {
            if (fieldType != null) {
                queries.add(fieldType.termQuery(value, context));
            } else {
                queries.add(new TermQuery(new Term(fieldName, BytesRefs.toBytesRef(value))));
            }
        }
        final LongValuesSource longValuesSource;
        if (minimumShouldMatchField != null) {
            MappedFieldType msmFieldType = context.fieldMapper(minimumShouldMatchField);
            if (msmFieldType == null) {
                throw new QueryShardException(context, "failed to find minimum_should_match field [" + minimumShouldMatchField + "]");
            }

            IndexNumericFieldData fieldData = context.getForField(msmFieldType);
            longValuesSource = new FieldValuesSource(fieldData);
        } else if (minimumShouldMatchScript != null) {
            SearchScript.Factory factory = context.getScriptService().compile(minimumShouldMatchScript, SearchScript.CONTEXT);
            Map<String, Object> params = new HashMap<>();
            params.putAll(minimumShouldMatchScript.getParams());
            params.put("num_terms", queries.size());
            SearchScript.LeafFactory leafFactory = factory.newFactory(params, context.lookup());
            longValuesSource = new ScriptLongValueSource(minimumShouldMatchScript, leafFactory);
        } else {
            throw new IllegalStateException("No minimum should match has been specified");
        }
        return new CoveringQuery(queries, longValuesSource);
    }

    static final class ScriptLongValueSource extends LongValuesSource {

        private final Script script;
        private final SearchScript.LeafFactory leafFactory;

        ScriptLongValueSource(Script script, SearchScript.LeafFactory leafFactory) {
            this.script = script;
            this.leafFactory = leafFactory;
        }

        @Override
        public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
            SearchScript searchScript = leafFactory.newInstance(ctx);
            return new LongValues() {
                @Override
                public long longValue() throws IOException {
                    return searchScript.runAsLong();
                }

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    searchScript.setDocument(doc);
                    return searchScript.run() != null;
                }
            };
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public int hashCode() {
            // CoveringQuery with this field value source cannot be cachable
            return System.identityHashCode(this);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public String toString() {
            return "script(" + script.toString() + ")";
        }

    }

    // Forked from LongValuesSource.FieldValuesSource and changed getValues() method to always use sorted numeric
    // doc values, because that is what is being used in NumberFieldMapper.
    static class FieldValuesSource extends LongValuesSource {

        private final IndexNumericFieldData field;

        FieldValuesSource(IndexNumericFieldData field) {
            this.field = field;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FieldValuesSource that = (FieldValuesSource) o;
            return Objects.equals(field, that.field);
        }

        @Override
        public String toString() {
            return "long(" + field + ")";
        }

        @Override
        public int hashCode() {
            return Objects.hash(field);
        }

        @Override
        public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
            SortedNumericDocValues values = field.load(ctx).getLongValues();
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
    }

}
