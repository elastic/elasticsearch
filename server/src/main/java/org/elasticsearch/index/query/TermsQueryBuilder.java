/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
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
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * A filter for a field based on several terms matching on any of them.
 */
public class TermsQueryBuilder extends AbstractQueryBuilder<TermsQueryBuilder> {
    public static final String NAME = "terms";

    private final String fieldName;
    private final BinaryValues values;
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
        this(fieldName, values != null ? Arrays.stream(values).boxed().toList() : null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, long... values) {
        this(fieldName, values != null ? Arrays.stream(values).boxed().toList() : null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, float... values) {
        this(fieldName, values != null ? IntStream.range(0, values.length).mapToObj(i -> values[i]).toList() : null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, double... values) {
        this(fieldName, values != null ? Arrays.stream(values).boxed().toList() : null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, Object... values) {
        this(fieldName, values != null ? Arrays.asList(values) : null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, Collection<?> values) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name cannot be null.");
        }
        if (values == null) {
            throw new IllegalArgumentException("No value specified for terms query");
        }
        this.fieldName = fieldName;
        if (values instanceof BinaryValues binaryValues) {
            this.values = binaryValues;
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
        this.values = in.readOptionalWriteable(BinaryValues::new);
        this.supplier = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (supplier != null) {
            throw new IllegalStateException("supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeOptionalWriteable(termsLookup);
        out.writeOptionalWriteable(values);
    }

    public String fieldName() {
        return this.fieldName;
    }

    public BinaryValues getValues() {
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
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
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
        return super.doRewrite(queryRewriteContext);
    }

    @Override
    protected QueryBuilder doIndexMetadataRewrite(QueryRewriteContext context) {
        MappedFieldType fieldType = context.getFieldType(this.fieldName);
        if (fieldType == null) {
            return new MatchNoneQueryBuilder("The \"" + getName() + "\" query is against a field that does not exist");
        }
        return maybeRewriteBasedOnConstantFields(fieldType, context);
    }

    @Override
    protected QueryBuilder doCoordinatorRewrite(CoordinatorRewriteContext coordinatorRewriteContext) {
        MappedFieldType fieldType = coordinatorRewriteContext.getFieldType(this.fieldName);
        // we don't rewrite a null field type to `match_none` on the coordinator because the coordinator has access
        // to only a subset of fields see {@link CoordinatorRewriteContext#getFieldType}
        return maybeRewriteBasedOnConstantFields(fieldType, coordinatorRewriteContext);
    }

    private QueryBuilder maybeRewriteBasedOnConstantFields(@Nullable MappedFieldType fieldType, QueryRewriteContext context) {
        if (fieldType instanceof ConstantFieldType constantFieldType) {
            // This logic is correct for all field types, but by only applying it to constant
            // fields we also have the guarantee that it doesn't perform I/O, which is important
            // since rewrites might happen on a network thread.
            Query query = constantFieldType.innerTermsQuery(values, context);
            if (query instanceof MatchAllDocsQuery) {
                return new MatchAllQueryBuilder();
            } else if (query instanceof MatchNoDocsQuery) {
                return new MatchNoneQueryBuilder("The \"" + getName() + "\" query was rewritten to a \"match_none\" query.");
            } else {
                assert false : "Constant fields must produce match-all or match-none queries, got " + query;
            }
        }
        return this;
    }

    /**
     * Store terms as a {@link BytesReference}.
     * <p>
     * When users send a query contain a lot of terms, A {@link BytesReference} can help
     * gc and reduce the cost of {@link #doWriteTo}, which can be slow for lots of terms.
     */
    @SuppressWarnings("rawtypes")
    public static final class BinaryValues extends AbstractCollection implements Writeable {

        @Nullable
        private BytesReference valueRef;

        private final boolean convert;

        private final Collection<?> values;

        private BinaryValues(StreamInput in) throws IOException {
            this.valueRef = null;
            this.convert = false;
            int ignored = in.readVInt();
            int ignored2 = in.readByte();
            assert ignored2 == StreamOutput.GENERIC_LIST_HEADER;
            values = in.readCollectionAsImmutableList(StreamInput::readGenericValue);
        }

        private BinaryValues(Collection<?> values, boolean convert) {
            this.convert = convert;
            this.values = values;
        }

        private static BytesReference serialize(Collection<?> values, boolean convert) {
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.writeByte(StreamOutput.GENERIC_LIST_HEADER);
                output.writeVInt(values.size());
                if (convert) {
                    for (Object value : values) {
                        output.writeGenericValue(AbstractQueryBuilder.maybeConvertToBytesRef(value));
                    }
                } else {
                    for (Object value : values) {
                        output.writeGenericValue(value);
                    }
                }
                return output.bytes();
            } catch (IOException e) {
                throw new UncheckedIOException("failed to serialize TermsQueryBuilder", e);
            }
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public Iterator<?> iterator() {
            var iter = values.iterator();
            return convert ? Iterators.map(iter, AbstractQueryBuilder::maybeConvertToBytesRef) : iter;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesReference(asBytes());
        }

        private BytesReference asBytes() {
            var ref = valueRef;
            if (ref == null) {
                ref = serialize(values, convert);
                valueRef = ref;
            }
            return ref;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return Iterators.equals(iterator(), ((BinaryValues) o).iterator(), Objects::equals);
        }

        @Override
        public int hashCode() {
            int hash = 1;
            for (Object o : this) {
                hash = 31 * hash + o.hashCode();
            }
            return hash;
        }

    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
