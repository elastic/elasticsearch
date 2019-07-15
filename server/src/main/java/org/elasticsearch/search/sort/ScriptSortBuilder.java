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

package org.elasticsearch.search.sort;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.AbstractBinaryDocValues;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.script.StringSortScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.search.sort.NestedSortBuilder.NESTED_FIELD;

/**
 * Script sort builder allows to sort based on a custom script expression.
 */
public class ScriptSortBuilder extends SortBuilder<ScriptSortBuilder> {

    public static final String NAME = "_script";
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField SCRIPT_FIELD = new ParseField("script");
    public static final ParseField SORTMODE_FIELD = new ParseField("mode");

    private final Script script;

    private final ScriptSortType type;

    private SortMode sortMode;

    private NestedSortBuilder nestedSort;

    /**
     * Constructs a script sort builder with the given script.
     *
     * @param script
     *            The script to use.
     * @param type
     *            The type of the script, can be either {@link ScriptSortType#STRING} or
     *            {@link ScriptSortType#NUMBER}
     */
    public ScriptSortBuilder(Script script, ScriptSortType type) {
        Objects.requireNonNull(script, "script cannot be null");
        Objects.requireNonNull(type, "type cannot be null");
        this.script = script;
        this.type = type;
    }

    ScriptSortBuilder(ScriptSortBuilder original) {
        this.script = original.script;
        this.type = original.type;
        this.order = original.order;
        this.sortMode = original.sortMode;
        this.nestedSort = original.nestedSort;
    }

    /**
     * Read from a stream.
     */
    public ScriptSortBuilder(StreamInput in) throws IOException {
        script = new Script(in);
        type = ScriptSortType.readFromStream(in);
        order = SortOrder.readFromStream(in);
        sortMode = in.readOptionalWriteable(SortMode::readFromStream);
        if (in.getVersion().before(Version.V_8_0_0)) {
            if (in.readOptionalNamedWriteable(QueryBuilder.class) != null || in.readOptionalString() != null) {
                throw new IOException("the [sort] options [nested_path] and [nested_filter] are removed in 8.x, " +
                    "please use [nested] instead");
            }
        }
        nestedSort = in.readOptionalWriteable(NestedSortBuilder::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        script.writeTo(out);
        type.writeTo(out);
        order.writeTo(out);
        out.writeOptionalWriteable(sortMode);
        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeOptionalString(null);
            out.writeOptionalNamedWriteable(null);
        }
        out.writeOptionalWriteable(nestedSort);
    }

    /**
     * Get the script used in this sort.
     */
    public Script script() {
        return this.script;
    }

    /**
     * Get the type used in this sort.
     */
    public ScriptSortType type() {
        return this.type;
    }

    /**
     * Defines which distance to use for sorting in the case a document contains multiple values.<br>
     * For {@link ScriptSortType#STRING}, the set of possible values is restricted to {@link SortMode#MIN} and {@link SortMode#MAX}
     */
    public ScriptSortBuilder sortMode(SortMode sortMode) {
        Objects.requireNonNull(sortMode, "sort mode cannot be null.");
        if (ScriptSortType.STRING.equals(type) && (sortMode == SortMode.SUM || sortMode == SortMode.AVG ||
                sortMode == SortMode.MEDIAN)) {
            throw new IllegalArgumentException("script sort of type [string] doesn't support mode [" + sortMode + "]");
        }
        this.sortMode = sortMode;
        return this;
    }

    /**
     * Get the sort mode.
     */
    public SortMode sortMode() {
        return this.sortMode;
    }

    /**
     * Returns the {@link NestedSortBuilder}
     */
    public NestedSortBuilder getNestedSort() {
        return this.nestedSort;
    }

    /**
     * Sets the {@link NestedSortBuilder} to be used for fields that are inside a nested
     * object. The {@link NestedSortBuilder} takes a `path` argument and an optional
     * nested filter that the nested objects should match with in
     * order to be taken into account for sorting.
     */
    public ScriptSortBuilder setNestedSort(final NestedSortBuilder nestedSort) {
        this.nestedSort = nestedSort;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params builderParams) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(SCRIPT_FIELD.getPreferredName(), script);
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(ORDER_FIELD.getPreferredName(), order);
        if (sortMode != null) {
            builder.field(SORTMODE_FIELD.getPreferredName(), sortMode);
        }
        if (nestedSort != null) {
            builder.field(NESTED_FIELD.getPreferredName(), nestedSort);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    private static ConstructingObjectParser<ScriptSortBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new ScriptSortBuilder((Script) a[0], (ScriptSortType) a[1]));

    static {
        PARSER.declareField(constructorArg(), (parser, context) -> Script.parse(parser),
                Script.SCRIPT_PARSE_FIELD, ValueType.OBJECT_OR_STRING);
        PARSER.declareField(constructorArg(), p -> ScriptSortType.fromString(p.text()), TYPE_FIELD, ValueType.STRING);
        PARSER.declareString((b, v) -> b.order(SortOrder.fromString(v)), ORDER_FIELD);
        PARSER.declareString((b, v) -> b.sortMode(SortMode.fromString(v)), SORTMODE_FIELD);
        PARSER.declareObject(ScriptSortBuilder::setNestedSort, (p, c) -> NestedSortBuilder.fromXContent(p), NESTED_FIELD);
    }

    /**
     * Creates a new {@link ScriptSortBuilder} from the query held by the {@link XContentParser} in
     * {@link org.elasticsearch.common.xcontent.XContent} format.
     *
     * @param parser the input parser. The state on the parser contained in this context will be changed as a side effect of this
     *        method call
     * @param elementName in some sort syntax variations the field name precedes the xContent object that specifies further parameters, e.g.
     *        in '{ "foo": { "order" : "asc"} }'. When parsing the inner object, the field name can be passed in via this argument
     */
    public static ScriptSortBuilder fromXContent(XContentParser parser, String elementName) {
        return PARSER.apply(parser, null);
    }


    @Override
    public SortFieldAndFormat build(QueryShardContext context) throws IOException {
        MultiValueMode valueMode = null;
        if (sortMode != null) {
            valueMode = MultiValueMode.fromString(sortMode.toString());
        }
        boolean reverse = (order == SortOrder.DESC);
        if (valueMode == null) {
            valueMode = reverse ? MultiValueMode.MAX : MultiValueMode.MIN;
        }

        Nested nested = null;
        if (nestedSort != null) {
            if (nestedSort.getNestedSort() != null && nestedSort.getMaxChildren() != Integer.MAX_VALUE)  {
                throw new QueryShardException(context,
                    "max_children is only supported on last level of nested sort");
            }
            nested = resolveNested(context, nestedSort);
        }

        final IndexFieldData.XFieldComparatorSource fieldComparatorSource;
        switch (type) {
            case STRING:
                final StringSortScript.Factory factory = context.getScriptService().compile(script, StringSortScript.CONTEXT);
                final StringSortScript.LeafFactory searchScript = factory.newFactory(script.getParams(), context.lookup());
                fieldComparatorSource = new BytesRefFieldComparatorSource(null, null, valueMode, nested) {
                    StringSortScript leafScript;
                    @Override
                    protected SortedBinaryDocValues getValues(LeafReaderContext context) throws IOException {
                        leafScript = searchScript.newInstance(context);
                        final BinaryDocValues values = new AbstractBinaryDocValues() {
                            final BytesRefBuilder spare = new BytesRefBuilder();
                            @Override
                            public boolean advanceExact(int doc) throws IOException {
                                leafScript.setDocument(doc);
                                return true;
                            }
                            @Override
                            public BytesRef binaryValue() {
                                spare.copyChars(leafScript.execute());
                                return spare.get();
                            }
                        };
                        return FieldData.singleton(values);
                    }
                    @Override
                    protected void setScorer(Scorable scorer) {
                        leafScript.setScorer(scorer);
                    }
                };
                break;
            case NUMBER:
                final NumberSortScript.Factory numberSortFactory = context.getScriptService().compile(script, NumberSortScript.CONTEXT);
                final NumberSortScript.LeafFactory numberSortScript = numberSortFactory.newFactory(script.getParams(), context.lookup());
                fieldComparatorSource = new DoubleValuesComparatorSource(null, Double.MAX_VALUE, valueMode, nested) {
                    NumberSortScript leafScript;
                    @Override
                    protected SortedNumericDoubleValues getValues(LeafReaderContext context) throws IOException {
                        leafScript = numberSortScript.newInstance(context);
                        final NumericDoubleValues values = new NumericDoubleValues() {
                            @Override
                            public boolean advanceExact(int doc) throws IOException {
                                leafScript.setDocument(doc);
                                return true;
                            }
                            @Override
                            public double doubleValue() {
                                return leafScript.execute();
                            }
                        };
                        return FieldData.singleton(values);
                    }
                    @Override
                    protected void setScorer(Scorable scorer) {
                        leafScript.setScorer(scorer);
                    }
                };
                break;
            default:
            throw new QueryShardException(context, "custom script sort type [" + type + "] not supported");
        }

        return new SortFieldAndFormat(new SortField("_script", fieldComparatorSource, reverse), DocValueFormat.RAW);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ScriptSortBuilder other = (ScriptSortBuilder) object;
        return Objects.equals(script, other.script) &&
                Objects.equals(type, other.type) &&
                Objects.equals(order, other.order) &&
                Objects.equals(sortMode, other.sortMode) &&
                Objects.equals(nestedSort, other.nestedSort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(script, type, order, sortMode, nestedSort);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public enum ScriptSortType implements Writeable {
        /** script sort for a string value **/
        STRING,
        /** script sort for a numeric value **/
        NUMBER;

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        /**
         * Read from a stream.
         */
        static ScriptSortType readFromStream(final StreamInput in) throws IOException {
            return in.readEnum(ScriptSortType.class);
        }

        public static ScriptSortType fromString(final String str) {
            Objects.requireNonNull(str, "input string is null");
            switch (str.toLowerCase(Locale.ROOT)) {
                case ("string"):
                    return ScriptSortType.STRING;
                case ("number"):
                    return ScriptSortType.NUMBER;
                default:
                    throw new IllegalArgumentException("Unknown ScriptSortType [" + str + "]");
            }
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    @Override
    public ScriptSortBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        if (nestedSort == null) {
            return this;
        }
        NestedSortBuilder rewrite = nestedSort.rewrite(ctx);
        if (nestedSort == rewrite) {
            return this;
        }
        return new ScriptSortBuilder(this).setNestedSort(rewrite);
    }
}
