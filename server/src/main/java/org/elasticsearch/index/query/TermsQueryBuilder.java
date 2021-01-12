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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ConstantFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.indices.TermsLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A filter for a field based on several terms matching on any of them.
 */
public class TermsQueryBuilder extends AbstractQueryBuilder<TermsQueryBuilder> {
    public static final String NAME = "terms";

    private final String fieldName;
    private final TermsLookup termsLookup;
    private final Supplier<List<?>> supplier;

    /**
     * Store terms as a {@link BytesRef}.
     * <p>
     * When users send a query contain a lot of terms, A {@link BytesRef} can help
     * gc and reduce the cost of {@link #doWriteTo}, which can be quite slow for many
     * terms.
     */
    private final BytesRef valueRef;

    /**
     * This is for lower version requests compatible.
     * <p>
     * If we do not keep this, it could be expensive when receiving a request from
     * lower version.
     * We have to read the value list by {@link StreamInput#readGenericValue},
     * serialize it into {@link #valueRef}, and then deserialize it again when
     * {@link #doToQuery} called}.
     * <p>
     * ToDo: remove in 9.0.0
     */
    private final List<?> values;
    private final boolean stale; //indicate if this comes from a lower version

    public TermsQueryBuilder(String fieldName, TermsLookup termsLookup) {
        this(fieldName, null, termsLookup);
    }

    /**
     * constructor used internally for serialization of both value / termslookup variants
     *
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
        //already converted in {@link fromXContent}
        this.valueRef = serialize(values, false);
        this.termsLookup = termsLookup;
        this.supplier = null;
        this.values = null;
        this.stale = false;
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
        this(fieldName, values != null ? Arrays.stream(values).mapToObj(s -> s).collect(Collectors.toList()) : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, long... values) {
        this(fieldName, values != null ? Arrays.stream(values).mapToObj(s -> s).collect(Collectors.toList()) : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, float... values) {
        this(fieldName, values != null ? IntStream.range(0, values.length)
                           .mapToObj(i -> values[i]).collect(Collectors.toList()) : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, double... values) {
        this(fieldName, values != null ? Arrays.stream(values).mapToObj(s -> s).collect(Collectors.toList()) : (Iterable<?>) null);
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
        this.valueRef = serialize(values);
        this.termsLookup = null;
        this.supplier = null;
        this.values = null;
        this.stale = false;
    }

    public TermsQueryBuilder(String fieldName, BytesRef valueRef) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name cannot be null.");
        }
        if (valueRef == null) {
            throw new IllegalArgumentException("valueRef can not be null");
        }
        this.fieldName = fieldName;
        this.valueRef = valueRef;
        this.termsLookup = null;
        this.supplier = null;
        this.values = null;
        this.stale = false;
    }

    private TermsQueryBuilder(String fieldName, Supplier<List<?>> supplier) {
        this.fieldName = fieldName;
        this.valueRef = null;
        this.termsLookup = null;
        this.supplier = supplier;
        this.values = null;
        this.stale = false;
    }

    /**
     * Read from a stream.
     */
    public TermsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        termsLookup = in.readOptionalWriteable(TermsLookup::new);
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            this.valueRef = in.readBytesRef();
            this.values = null;
            this.stale = false;
        } else {
            this.values = (List<?>) in.readGenericValue();
            this.valueRef = null;
            this.stale = true;
        }
        this.supplier = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (supplier != null) {
            throw new IllegalStateException("supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeOptionalWriteable(termsLookup);
        if (stale) {
            out.writeGenericValue(this.values);
        } else {
            if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
                out.writeBytesRef(valueRef);
            } else {
                out.write(valueRef.bytes, valueRef.offset, valueRef.length);
            }
        }
    }

    /**
     * package private for testing purpose
     */
    boolean isStale() {
        return stale;
    }

    public String fieldName() {
        return this.fieldName;
    }

    public List<Object> values() {
        return stale ? TermsSetQueryBuilder.convertBack(this.values) : deserialize(valueRef, true);
    }

    public BytesRef valueRef() {
        return valueRef;
    }

    public TermsLookup termsLookup() {
        return this.termsLookup;
    }

    private boolean noTerms() {
        assert termsLookup == null && supplier == null : "terms not fetched, can not check terms number";
        if (stale) {
            return values == null || values.isEmpty();
        } else {
            return valueRef.length == 0;
        }
    }

    /**
     * Same as {@link #serialize(List, boolean)} but on an {@link Iterable} and always convert.
     */
    private static BytesRef serialize(Iterable<?> values) {
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
        return serialize(list, true);
    }

    /**
     * serialize a {@link List} to {@link BytesRef}
     * @param list a {@link List} to serialize
     * @return a {@link BytesRef} serialized from the list
     */
    private static BytesRef serialize(List<?> list, boolean convert) {
        if (list == null || list.isEmpty()) {
            return new BytesRef();
        }
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            if (convert) {
                list = list.stream().map(AbstractQueryBuilder::maybeConvertToBytesRef).collect(Collectors.toList());
            }
            output.writeGenericValue(list);
            return output.bytes().toBytesRef();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to serialize TermsQueryBuilder", e);
        }
    }

    /**
     * deserialize a {@link BytesRef} to a {@link List}
     * @param bytesRef {@link BytesRef} to deserialize
     * @param readable if the list element need to be readable
     * @return a {@link List} deserialized
     */
    @SuppressWarnings("unchecked")
    private static List<Object> deserialize(BytesRef bytesRef, boolean readable) {
        if (bytesRef.length == 0) {
            return Collections.emptyList();
        }
        List<Object> list;
        try (StreamInput streamInput = StreamInput.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length)) {
            list = (List<Object>) streamInput.readGenericValue();
        } catch (IOException e) {
            throw new UncheckedIOException("fail to deserialize TermsQueryBuilder", e);
        }
        return readable ? TermsSetQueryBuilder.convertBack(list) : list;
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
                if  (fieldName != null) {
                    throw new ParsingException(parser.getTokenLocation(),
                            "[" + TermsQueryBuilder.NAME + "] query does not support multiple fields");
                }
                fieldName = currentFieldName;
                values = parseValues(parser);
            } else if (token == XContentParser.Token.START_OBJECT) {
                if  (fieldName != null) {
                    throw new ParsingException(parser.getTokenLocation(),
                            "[" + TermsQueryBuilder.NAME + "] query does not support more than one field. "
                            + "Already got: [" + fieldName + "] but also found [" + currentFieldName +"]");
                }
                fieldName = currentFieldName;
                termsLookup = TermsLookup.parseTermsLookup(parser);
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "[" + TermsQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "[" + TermsQueryBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
            }
        }

        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + TermsQueryBuilder.NAME + "] query requires a field name, " +
                    "followed by array of terms or a document lookup specification");
        }

        TermsQueryBuilder builder = new TermsQueryBuilder(fieldName, values, termsLookup)
            .boost(boost)
            .queryName(queryName);

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
    @SuppressWarnings("unchecked")
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (termsLookup != null || supplier != null || noTerms()) {
            throw new UnsupportedOperationException("query must be rewritten first");
        }
        int maxTermsCount = context.getIndexSettings().getMaxTermsCount();
        List<Object> values = stale ? (List<Object>) this.values : deserialize(valueRef, false);
        if (values.size() > maxTermsCount) {
            throw new IllegalArgumentException(
                "The number of terms ["  + values.size() +  "] used in the Terms Query request has exceeded " +
                    "the allowed maximum of [" + maxTermsCount + "]. " + "This maximum can be set by changing the [" +
                    IndexSettings.MAX_TERMS_COUNT_SETTING.getKey() + "] index level setting.");
        }
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType == null) {
            throw new IllegalStateException("Rewrite first");
        }
        return fieldType.termsQuery(values, context);
    }

    private void fetch(TermsLookup termsLookup, Client client, ActionListener<List<Object>> actionListener) {
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
        return Objects.hash(fieldName, valueRef, termsLookup, supplier, values);
    }

    @Override
    protected boolean doEquals(TermsQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
            Objects.equals(valueRef, other.valueRef) &&
            Objects.equals(termsLookup, other.termsLookup) &&
            Objects.equals(supplier, other.supplier) &&
            Objects.equals(values, other.values);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        if (supplier != null) {
            return supplier.get() == null ? this : new TermsQueryBuilder(this.fieldName, supplier.get());
        } else if (this.termsLookup != null) {
            SetOnce<List<?>> supplier = new SetOnce<>();
            queryRewriteContext.registerAsyncAction((client, listener) ->
                fetch(termsLookup, client, listener.map(list -> {
                supplier.set(list);
                return null;
            })));
            return new TermsQueryBuilder(this.fieldName, supplier::get);
        }
        if (noTerms()) {
            return new MatchNoneQueryBuilder();
        }

        QueryShardContext context = queryRewriteContext.convertToShardContext();
        if (context != null) {
            MappedFieldType fieldType = context.getFieldType(this.fieldName);
            if (fieldType == null) {
                return new MatchNoneQueryBuilder();
            } else if (fieldType instanceof ConstantFieldType) {
                // This logic is correct for all field types, but by only applying it to constant
                // fields we also have the guarantee that it doesn't perform I/O, which is important
                // since rewrites might happen on a network thread.
                List<Object> values = stale ? (List<Object>) this.values : deserialize(valueRef, false);
                Query query = fieldType.termsQuery(values, context);
                if (query instanceof MatchAllDocsQuery) {
                    return new MatchAllQueryBuilder();
                } else if (query instanceof MatchNoDocsQuery) {
                    return new MatchNoneQueryBuilder();
                } else {
                    assert false : "Constant fields must produce match-all or match-none queries, got " + query ;
                }
            }
        }

        return this;
    }
}
