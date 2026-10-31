/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ConstantFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.CharBuffer;
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
public class TermsQueryBuilder extends LeafQueryBuilder<TermsQueryBuilder> {
    public static final String NAME = "terms";

    private final String fieldName;
    private final BinaryValues values;
    private final TermsLookup termsLookup;
    private final Supplier<BinaryValues> supplier;

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

    private TermsQueryBuilder(String fieldName, Supplier<BinaryValues> supplier) {
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

        return new TermsQueryBuilder(fieldName, values, termsLookup).boost(boost).queryName(queryName);
    }

    static List<Object> parseValues(XContentParser parser) throws IOException {
        List<Object> values = new ArrayList<>();
        final TermEncoder encoder = new TermEncoder();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            final Object parsed = parser.objectBytes();
            // A term longer than the limit has more bytes than that however it is encoded, so it takes the general path, which rejects it,
            // instead of growing the encoder's buffer for it.
            final Object value = parsed instanceof CharBuffer chars && chars.length() <= IndexWriter.MAX_TERM_LENGTH
                ? BytesRefs.checkIndexableLength(encoder.encode(chars))
                : maybeConvertToBytesRef(parsed);
            if (value == null) {
                throw new ParsingException(parser.getTokenLocation(), "No value specified for terms query");
            }
            values.add(value);
        }
        return values;
    }

    /**
     * Encodes the terms of one list as UTF-8 into arrays of exactly their size.
     * This allocates a temporary scratch buffer for the worst case of 3 bytes per char.
     * The optimized route checks if the chars are all ASCII, and if so, encodes them directly.
     */
    private static final class TermEncoder {
        private byte[] scratch = new byte[128];

        BytesRef encode(CharBuffer value) {
            final int length = value.length();
            if (value.hasArray() == false) {
                final int used = UnicodeUtil.UTF16toUTF8(value, 0, length, ensureScratch(length));
                return new BytesRef(Arrays.copyOf(scratch, used));
            }
            final char[] chars = value.array();
            final int from = value.arrayOffset() + value.position();
            // Shortcut - check the first char for free before allocating
            if (length > 0 && chars[from] > 0x7F) {
                final int used = UnicodeUtil.UTF16toUTF8(chars, from, length, ensureScratch(length));
                return new BytesRef(Arrays.copyOf(scratch, used));
            }
            final byte[] bytes = new byte[length];
            int bits = 0;
            for (int i = 0; i < length; i++) {
                final char c = chars[from + i];
                bits |= c;
                bytes[i] = (byte) c;
            }
            if (bits < 0x80) {
                // If the whole string is ASCII, we can avoid the UTF-8 conversion
                return new BytesRef(bytes);
            }
            final int used = UnicodeUtil.UTF16toUTF8(chars, from, length, ensureScratch(length));
            return new BytesRef(Arrays.copyOf(scratch, used));
        }

        // Ensure scratch is large enough to hold the UTF-8 encoded bytes
        private byte[] ensureScratch(int charLength) {
            final int max = UnicodeUtil.maxUTF8Length(charLength);
            if (scratch.length < max) {
                scratch = new byte[ArrayUtil.oversize(max, Byte.BYTES)];
            }
            return scratch;
        }
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
        // Checked on the shard against this index's own max_terms_count. Inline terms values are only capped
        // here; lookup values are also capped earlier, while the lookup is resolved.
        int maxTermsCount = context.getIndexSettings().getMaxTermsCount();
        if (values.size() > maxTermsCount) {
            throw tooManyTermsException(values.size(), maxTermsCount);
        }
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType == null) {
            throw new IllegalStateException("Rewrite first");
        }
        values.chargeDecodedBytes(context);
        return fieldType.termsQuery(values, context);
    }

    private static IllegalArgumentException tooManyTermsException(int termsCount, int maxTermsCount) {
        return new IllegalArgumentException(
            "The number of terms ["
                + termsCount
                + "] used in the Terms Query request has exceeded "
                + "the allowed maximum of ["
                + maxTermsCount
                + "]. "
                + "This maximum can be set by changing the ["
                + IndexSettings.MAX_TERMS_COUNT_SETTING.getKey()
                + "] index level setting."
        );
    }

    /**
     * Resolves the {@code index.max_terms_count} limit to apply while rewriting on the coordinating node, which has no single
     * index scope. The most permissive limit across the resolved local indices is returned so this early check never rejects a
     * value list that a target shard would accept; each shard still enforces its own limit when the query is built. When no
     * local index metadata is available, {@link Integer#MAX_VALUE} is returned, leaving enforcement to the shard-level check.
     */
    private static int resolveMaxTermsCount(QueryRewriteContext context) {
        ResolvedIndices resolvedIndices = context.getResolvedIndices();
        if (resolvedIndices == null) {
            return Integer.MAX_VALUE;
        }
        Collection<IndexMetadata> localIndices = resolvedIndices.getConcreteLocalIndicesMetadata().values();
        if (localIndices.isEmpty()) {
            return Integer.MAX_VALUE;
        }
        int maxTermsCount = 0;
        for (IndexMetadata indexMetadata : localIndices) {
            maxTermsCount = Math.max(maxTermsCount, IndexSettings.MAX_TERMS_COUNT_SETTING.get(indexMetadata.getSettings()));
        }
        return maxTermsCount;
    }

    /**
     * Resolves a {@code terms} lookup: fetches the referenced document and extracts the values at the lookup path.
     * <p>
     * Registered as a unique async action ({@link QueryRewriteContext#registerUniqueAsyncAction}) so that clauses pointing at
     * the same document (same index, id, path and routing) share one fetch instead of fetching per clause. The
     * {@code index.max_terms_count} limit is applied here, so an oversized lookup is rejected before the query is built.
     */
    private static final class TermsLookupFetch extends QueryRewriteAsyncAction<BinaryValues, TermsLookupFetch> {
        private final TermsLookup termsLookup;
        // The limit is the same for every clause of a request, so it is not part of the dedup identity below.
        private final int maxTermsCount;

        TermsLookupFetch(TermsLookup termsLookup, int maxTermsCount) {
            this.termsLookup = termsLookup;
            this.maxTermsCount = maxTermsCount;
        }

        @Override
        protected void execute(Client client, ActionListener<BinaryValues> listener) {
            GetRequest getRequest = new GetRequest(termsLookup.index(), termsLookup.id());
            getRequest.preference("_local").routing(termsLookup.routing());
            client.get(getRequest, listener.map(getResponse -> {
                if (getResponse.isSourceEmpty()) {
                    return new BinaryValues(List.of(), false);
                }
                // extracted values come straight from the document source, so they still need converting (unlike inline values,
                // which are already converted while parsing the query)
                List<Object> extractedValues = XContentMapValues.extractRawValues(termsLookup.path(), getResponse.getSourceAsMap());
                if (extractedValues.size() > maxTermsCount) {
                    // reject here, during rewrite, before the values are copied into every clause and serialized to the data nodes
                    throw tooManyTermsException(extractedValues.size(), maxTermsCount);
                }
                return new BinaryValues(extractedValues, true);
            }));
        }

        @Override
        public int doHashCode() {
            return termsLookup.hashCode();
        }

        @Override
        public boolean doEquals(TermsLookupFetch other) {
            return termsLookup.equals(other.termsLookup);
        }
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
            SetOnce<BinaryValues> supplier = new SetOnce<>();
            queryRewriteContext.registerUniqueAsyncAction(
                new TermsLookupFetch(termsLookup, resolveMaxTermsCount(queryRewriteContext)),
                supplier::set
            );
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

        private static final long BYTES_REF_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);

        @Nullable
        private BytesReference valueRef;

        private final boolean convert;

        private final Collection<?> values;

        /**
         * Heap the values take after deserialization beyond their serialized size. The serialized size is already held by the
         * in-flight requests breaker until the request that carried them responds, so only the difference is charged.
         */
        private final long decodedBytesOverWire;

        // guarded by this
        private int decodedBytesHolders;
        @Nullable
        private CircuitBreaker decodedBytesBreaker;

        private BinaryValues(StreamInput in) throws IOException {
            this.valueRef = null;
            this.convert = false;
            int serializedBytes = in.readVInt();
            int ignored2 = in.readByte();
            assert ignored2 == StreamOutput.GENERIC_LIST_HEADER;
            List<Object> decoded = in.readCollectionAsImmutableList(StreamInput::readGenericValue);
            values = decoded;
            decodedBytesOverWire = Math.max(0L, ramBytesUsed(decoded) - serializedBytes);
        }

        private BinaryValues(Collection<?> values, boolean convert) {
            this.convert = convert;
            this.values = values;
            this.decodedBytesOverWire = 0L;
        }

        private static long ramBytesUsed(List<Object> values) {
            long bytes = RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * values.size()
            );
            for (Object value : values) {
                if (value instanceof BytesRef bytesRef) {
                    bytes += BYTES_REF_SHALLOW_SIZE + RamUsageEstimator.sizeOf(bytesRef.bytes);
                } else {
                    bytes += RamUsageEstimator.sizeOfObject(value, 0);
                }
            }
            return bytes;
        }

        /**
         * Charges {@link #decodedBytesOverWire} to the request breaker once per deserialized copy, held while at least one search
         * context that built a query from it is open. With the batched query phase every shard search of a node shares one copy,
         * so a per-context charge would count the same memory once per shard.
         */
        private void chargeDecodedBytes(SearchExecutionContext context) {
            CircuitBreaker breaker = context.getCircuitBreaker();
            if (decodedBytesOverWire == 0L || breaker == null) {
                return;
            }
            synchronized (this) {
                if (decodedBytesHolders == 0) {
                    breaker.addEstimateBytesAndMaybeBreak(decodedBytesOverWire, ChildMemoryCircuitBreaker.CATEGORY_TERMS);
                    decodedBytesBreaker = breaker;
                } else {
                    assert decodedBytesBreaker == breaker : "decoded terms shared by contexts with different breakers";
                }
                decodedBytesHolders++;
            }
            context.addQueryConstructionReleasable(Releasables.releaseOnce(this::releaseDecodedBytes));
        }

        private synchronized void releaseDecodedBytes() {
            assert decodedBytesHolders > 0 : "decoded terms released more often than charged";
            if (--decodedBytesHolders == 0) {
                assert decodedBytesBreaker != null : "charged decoded terms without a breaker";
                decodedBytesBreaker.addWithoutBreaking(-decodedBytesOverWire, ChildMemoryCircuitBreaker.CATEGORY_TERMS);
                decodedBytesBreaker = null;
            }
        }

        // Package-private for testing.
        long decodedBytesOverWire() {
            return decodedBytesOverWire;
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
        return TransportVersion.zero();
    }
}
