/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ConstantFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * A filter for a field based on several terms matching on any of them.
 */
public class TermsQueryBuilder extends AbstractQueryBuilder<TermsQueryBuilder> {
    public static final String NAME = "terms";
    private static final TransportVersion VERSION_STORE_VALUES_AS_BYTES_REFERENCE = TransportVersion.V_7_12_0;

    private final String fieldName;
    private final Values values;
    private final TermsLookup termsLookup;
    private final Supplier<List<?>> supplier;

    public TermsQueryBuilder(String fieldName, TermsLookup termsLookup) {
        this(fieldName, null, termsLookup);
    }

    /**
     * constructor used internally for serialization of both value / termslookup variants
     */
    private TermsQueryBuilder(String fieldName, List<Object> values, TermsLookup termsLookup) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name cannot be null.");
        }
        if (values == null && termsLookup == null) {
            throw new IllegalArgumentException("No value or termsLookup specified for terms query");
        }
        if (values != null && termsLookup != null) {
            throw new IllegalArgumentException("Both values and termsLookup specified for terms query");
        }
        this.fieldName = fieldName;
        // already converted in {@link fromXContent}
        this.values = values == null ? null : new BinaryValues(values, false);
        this.termsLookup = termsLookup;
        this.supplier = null;
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, String... values) {
        this(fieldName, values != null ? Arrays.asList(values) : null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, int... values) {
        this(fieldName, values != null ? Arrays.stream(values).mapToObj(s -> s).toList() : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, long... values) {
        this(fieldName, values != null ? Arrays.stream(values).mapToObj(s -> s).toList() : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, float... values) {
        this(fieldName, values != null ? IntStream.range(0, values.length).mapToObj(i -> values[i]).toList() : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, double... values) {
        this(fieldName, values != null ? Arrays.stream(values).mapToObj(s -> s).toList() : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, Object... values) {
        this(fieldName, values != null ? Arrays.asList(values) : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, Iterable<?> values) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name cannot be null.");
        }
        if (values == null) {
            throw new IllegalArgumentException("No value specified for terms query");
        }
        this.fieldName = fieldName;
        if (values instanceof Values) {
            this.values = (Values) values;
        } else {
            this.values = new BinaryValues(values, true);
        }
        this.termsLookup = null;
        this.supplier = null;
    }

    private TermsQueryBuilder(String fieldName, Supplier<List<?>> supplier) {
        this.fieldName = fieldName;
        this.values = null;
        this.termsLookup = null;
        this.supplier = supplier;
    }

    /**
     * Read from a stream.
     */
    public TermsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.termsLookup = in.readOptionalWriteable(TermsLookup::new);
        this.values = Values.readFrom(in);
        this.supplier = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (supplier != null) {
            throw new IllegalStateException("supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeOptionalWriteable(termsLookup);
        Values.writeTo(out, values);
    }

    public String fieldName() {
        return this.fieldName;
    }

    public Values getValues() {
        return values;
    }

    /**
     * get readable values
     * only for {@link #toXContent} and tests, don't use this to construct a query.
     * use {@link #getValues()} instead.
     */
    public List<Object> values() {
        List<Object> readableValues = new ArrayList<>();
        for (Object value : values) {
            readableValues.add(AbstractQueryBuilder.maybeConvertToString(value));
        }
        return readableValues;
    }

    public TermsLookup termsLookup() {
        return this.termsLookup;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (this.termsLookup != null) {
            builder.startObject(fieldName);
            termsLookup.toXContent(builder, params);
            builder.endObject();
        } else {
            builder.field(fieldName, values());
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static TermsQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        List<Object> values = null;
        TermsLookup termsLookup = null;

        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (fieldName != null) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + TermsQueryBuilder.NAME + "] query does not support multiple fields"
                    );
                }
                fieldName = currentFieldName;
                values = parseValues(parser);
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (fieldName != null) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "["
                            + TermsQueryBuilder.NAME
                            + "] query does not support more than one field. "
                            + "Already got: ["
                            + fieldName
                            + "] but also found ["
                            + currentFieldName
                            + "]"
                    );
                }
                fieldName = currentFieldName;
                termsLookup = TermsLookup.parseTermsLookup(parser);
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + TermsQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + TermsQueryBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }

        if (fieldName == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "["
                    + TermsQueryBuilder.NAME
                    + "] query requires a field name, "
                    + "followed by array of terms or a document lookup specification"
            );
        }

        TermsQueryBuilder builder = new TermsQueryBuilder(fieldName, values, termsLookup).boost(boost).queryName(queryName);

        return builder;
    }

    static List<Object> parseValues(XContentParser parser) throws IOException {
        List<Object> values = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            Object value = maybeConvertToBytesRef(parser.objectBytes());
            if (value == null) {
                throw new ParsingException(parser.getTokenLocation(), "No value specified for terms query");
            }
            values.add(value);
        }
        return values;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if (termsLookup != null || supplier != null || values == null || values.isEmpty()) {
            throw new UnsupportedOperationException("query must be rewritten first");
        }
        int maxTermsCount = context.getIndexSettings().getMaxTermsCount();
        if (values.size() > maxTermsCount) {
            throw new IllegalArgumentException(
                "The number of terms ["
                    + values.size()
                    + "] used in the Terms Query request has exceeded "
                    + "the allowed maximum of ["
                    + maxTermsCount
                    + "]. "
                    + "This maximum can be set by changing the ["
                    + IndexSettings.MAX_TERMS_COUNT_SETTING.getKey()
                    + "] index level setting."
            );
        }
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType == null) {
            throw new IllegalStateException("Rewrite first");
        }
        return fieldType.termsQuery(values, context);
    }

    private static void fetch(TermsLookup termsLookup, Client client, ActionListener<List<Object>> actionListener) {
        GetRequest getRequest = new GetRequest(termsLookup.index(), termsLookup.id());
        getRequest.preference("_local").routing(termsLookup.routing());
        client.get(getRequest, actionListener.map(getResponse -> {
            List<Object> terms = new ArrayList<>();
            if (getResponse.isSourceEmpty() == false) { // extract terms only if the doc source exists
                List<Object> extractedValues = XContentMapValues.extractRawValues(termsLookup.path(), getResponse.getSourceAsMap());
                terms.addAll(extractedValues);
            }
            return terms;
        }));
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, values, termsLookup, supplier);
    }

    @Override
    protected boolean doEquals(TermsQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(values, other.values)
            && Objects.equals(termsLookup, other.termsLookup)
            && Objects.equals(supplier, other.supplier);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        if (supplier != null) {
            return supplier.get() == null ? this : new TermsQueryBuilder(this.fieldName, supplier.get());
        } else if (this.termsLookup != null) {
            SetOnce<List<?>> supplier = new SetOnce<>();
            queryRewriteContext.registerAsyncAction((client, listener) -> fetch(termsLookup, client, listener.map(list -> {
                supplier.set(list);
                return null;
            })));
            return new TermsQueryBuilder(this.fieldName, supplier::get);
        }

        if (values == null || values.isEmpty()) {
            return new MatchNoneQueryBuilder("The \"" + getName() + "\" query was rewritten to a \"match_none\" query.");
        }

        SearchExecutionContext context = queryRewriteContext.convertToSearchExecutionContext();
        if (context != null) {
            MappedFieldType fieldType = context.getFieldType(this.fieldName);
            if (fieldType == null) {
                return new MatchNoneQueryBuilder("The \"" + getName() + "\" query was rewritten to a \"match_none\" query.");
            } else if (fieldType instanceof ConstantFieldType) {
                // This logic is correct for all field types, but by only applying it to constant
                // fields we also have the guarantee that it doesn't perform I/O, which is important
                // since rewrites might happen on a network thread.
                Query query = fieldType.termsQuery(values, context);
                if (query instanceof MatchAllDocsQuery) {
                    return new MatchAllQueryBuilder();
                } else if (query instanceof MatchNoDocsQuery) {
                    return new MatchNoneQueryBuilder("The \"" + getName() + "\" query was rewritten to a \"match_none\" query.");
                } else {
                    assert false : "Constant fields must produce match-all or match-none queries, got " + query;
                }
            }
        }

        return this;
    }

    @SuppressWarnings("rawtypes")
    private abstract static class Values extends AbstractCollection implements Writeable {

        private static Values readFrom(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(VERSION_STORE_VALUES_AS_BYTES_REFERENCE)) {
                return in.readOptionalWriteable(BinaryValues::new);
            } else {
                List<?> list = (List<?>) in.readGenericValue();
                return list == null ? null : new ListValues(list);
            }
        }

        private static void writeTo(StreamOutput out, Values values) throws IOException {
            if (out.getTransportVersion().onOrAfter(VERSION_STORE_VALUES_AS_BYTES_REFERENCE)) {
                out.writeOptionalWriteable(values);
            } else {
                if (values == null) {
                    out.writeGenericValue(null);
                } else {
                    values.writeTo(out);
                }
            }
        }

        protected static BytesReference serialize(Iterable<?> values, boolean convert) {
            List<?> list;
            if (values instanceof List<?>) {
                list = (List<?>) values;
            } else {
                ArrayList<Object> arrayList = new ArrayList<>();
                for (Object o : values) {
                    arrayList.add(o);
                }
                list = arrayList;
            }
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                if (convert) {
                    list = list.stream().map(AbstractQueryBuilder::maybeConvertToBytesRef).toList();
                }
                output.writeGenericValue(list);
                return output.bytes();
            } catch (IOException e) {
                throw new UncheckedIOException("failed to serialize TermsQueryBuilder", e);
            }
        }

        @Override
        public final boolean add(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public final boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public final boolean containsAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public final boolean addAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public final boolean removeAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public final boolean retainAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public final void clear() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Store terms as a {@link BytesReference}.
     * <p>
     * When users send a query contain a lot of terms, A {@link BytesReference} can help
     * gc and reduce the cost of {@link #doWriteTo}, which can be slow for lots of terms.
     */
    @SuppressWarnings("rawtypes")
    private static class BinaryValues extends Values {

        private final BytesReference valueRef;
        private final int size;

        private BinaryValues(StreamInput in) throws IOException {
            this(in.readBytesReference());
        }

        private BinaryValues(Iterable<?> values, boolean convert) {
            this(serialize(values, convert));
        }

        private BinaryValues(BytesReference bytesRef) {
            this.valueRef = bytesRef;
            try (StreamInput in = valueRef.streamInput()) {
                size = consumerHeadersAndGetListSize(in);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Iterator iterator() {
            return new Iterator<>() {
                private final StreamInput in;
                private int pos = 0;

                {
                    try {
                        in = valueRef.streamInput();
                        consumerHeadersAndGetListSize(in);
                    } catch (IOException e) {
                        throw new UncheckedIOException("failed to deserialize TermsQueryBuilder", e);
                    }
                }

                @Override
                public boolean hasNext() {
                    return pos < size;
                }

                @Override
                public Object next() {
                    try {
                        pos++;
                        return in.readGenericValue();
                    } catch (IOException e) {
                        throw new UncheckedIOException("failed to deserialize TermsQueryBuilder", e);
                    }
                }
            };
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(VERSION_STORE_VALUES_AS_BYTES_REFERENCE)) {
                out.writeBytesReference(valueRef);
            } else {
                valueRef.writeTo(out);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BinaryValues that = (BinaryValues) o;
            return Objects.equals(valueRef, that.valueRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(valueRef);
        }

        private static int consumerHeadersAndGetListSize(StreamInput in) throws IOException {
            byte genericSign = in.readByte();
            assert genericSign == 7;
            return in.readVInt();
        }
    }

    /**
     * This is for lower version requests compatible.
     * <p>
     * If we do not keep this, it could be expensive when receiving a request from
     * lower version.
     * We have to read the value list by {@link StreamInput#readGenericValue},
     * serialize it into {@link BytesReference}, and then deserialize it again when
     * {@link #doToQuery} called}.
     * <p>
     *
     * TODO: remove in 9.0.0
     */
    @SuppressWarnings("rawtypes")
    private static class ListValues extends Values {

        private final List<?> values;

        private ListValues(List<?> values) throws IOException {
            this.values = values;
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public boolean contains(Object o) {
            return values.contains(o);
        }

        @Override
        public Iterator iterator() {
            return values.iterator();
        }

        @Override
        public Object[] toArray() {
            return values.toArray();
        }

        @Override
        public Object[] toArray(Object[] a) {
            return values.toArray(a);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object[] toArray(IntFunction generator) {
            return values.toArray(generator);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(VERSION_STORE_VALUES_AS_BYTES_REFERENCE)) {
                BytesReference bytesRef = serialize(values, false);
                out.writeBytesReference(bytesRef);
            } else {
                out.writeGenericValue(values);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ListValues that = (ListValues) o;
            return Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(values);
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }
}
