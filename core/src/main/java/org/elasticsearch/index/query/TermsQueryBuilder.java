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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.indices.TermsLookup;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A filter for a field based on several terms matching on any of them.
 */
public class TermsQueryBuilder extends AbstractQueryBuilder<TermsQueryBuilder> {
    public static final String NAME = "terms";

    private final String fieldName;
    private final List<?> values;
    private final TermsLookup termsLookup;
    private final Supplier<List<?>> supplier;

    public TermsQueryBuilder(String fieldName, TermsLookup termsLookup) {
        this(fieldName, null, termsLookup);
    }

    /**
     * constructor used internally for serialization of both value / termslookup variants
     */
    TermsQueryBuilder(String fieldName, List<Object> values, TermsLookup termsLookup) {
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
        this.values = values == null ? null : convert(values);
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
        this.values = convert(values);
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
        fieldName = in.readString();
        termsLookup = in.readOptionalWriteable(TermsLookup::new);
        values = (List<?>) in.readGenericValue();
        this.supplier = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (supplier != null) {
            throw new IllegalStateException("supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeOptionalWriteable(termsLookup);
        out.writeGenericValue(values);
    }

    public String fieldName() {
        return this.fieldName;
    }

    public List<Object> values() {
        return convertBack(this.values);
    }

    public TermsLookup termsLookup() {
        return this.termsLookup;
    }

    private static final Set<Class<? extends Number>> INTEGER_TYPES = new HashSet<>(
            Arrays.asList(Byte.class, Short.class, Integer.class, Long.class));
    private static final Set<Class<?>> STRING_TYPES = new HashSet<>(
            Arrays.asList(BytesRef.class, String.class));

    /**
     * Same as {@link #convert(List)} but on an {@link Iterable}.
     */
    private static List<?> convert(Iterable<?> values) {
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
        return convert(list);
    }

    /**
     * Convert the list in a way that optimizes storage in the case that all
     * elements are either integers or {@link String}s/{@link BytesRef}s. This
     * is useful to help garbage collections for use-cases that involve sending
     * very large terms queries to Elasticsearch. If the list does not only
     * contain integers or {@link String}s, then a list is returned where all
     * {@link String}s have been replaced with {@link BytesRef}s.
     */
    static List<?> convert(List<?> list) {
        if (list.isEmpty()) {
            return Collections.emptyList();
        }

        final boolean allNumbers = list.stream().allMatch(o -> o != null && INTEGER_TYPES.contains(o.getClass()));
        if (allNumbers) {
            final long[] elements = list.stream().mapToLong(o -> ((Number) o).longValue()).toArray();
            return new AbstractList<Object>() {
                @Override
                public Object get(int index) {
                    return elements[index];
                }
                @Override
                public int size() {
                    return elements.length;
                }
            };
        }

        final boolean allStrings = list.stream().allMatch(o -> o != null && STRING_TYPES.contains(o.getClass()));
        if (allStrings) {
            final BytesRefBuilder builder = new BytesRefBuilder();
            try (BytesStreamOutput bytesOut = new BytesStreamOutput()) {
                final int[] endOffsets = new int[list.size()];
                int i = 0;
                for (Object o : list) {
                    BytesRef b;
                    if (o instanceof BytesRef) {
                        b = (BytesRef) o;
                    } else {
                        builder.copyChars(o.toString());
                        b = builder.get();
                    }
                    bytesOut.writeBytes(b.bytes, b.offset, b.length);
                    if (i == 0) {
                        endOffsets[0] = b.length;
                    } else {
                        endOffsets[i] = Math.addExact(endOffsets[i-1], b.length);
                    }
                    ++i;
                }
                final BytesReference bytes = bytesOut.bytes();
                return new AbstractList<Object>() {
                    @Override
                    public Object get(int i) {
                        final int startOffset = i == 0 ? 0 : endOffsets[i-1];
                        final int endOffset = endOffsets[i];
                        return bytes.slice(startOffset, endOffset - startOffset).toBytesRef();
                    }
                    @Override
                    public int size() {
                        return endOffsets.length;
                    }
                };
            }
        }

        return list.stream().map(o -> o instanceof String ? new BytesRef(o.toString()) : o).collect(Collectors.toList());
    }

    /**
     * Convert the internal {@link List} of values back to a user-friendly list.
     * Integers are kept as-is since the terms query does not make any difference
     * between {@link Integer}s and {@link Long}s, but {@link BytesRef}s are
     * converted back to {@link String}s.
     */
    static List<Object> convertBack(List<?> list) {
        return new AbstractList<Object>() {
            @Override
            public int size() {
                return list.size();
            }
            @Override
            public Object get(int index) {
                Object o = list.get(index);
                if (o instanceof BytesRef) {
                    o = ((BytesRef) o).utf8ToString();
                }
                // we do not convert longs, all integer types are equivalent
                // as far as this query is concerned
                return o;
            }
        };
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (this.termsLookup != null) {
            builder.startObject(fieldName);
            termsLookup.toXContent(builder, params);
            builder.endObject();
        } else {
            builder.field(fieldName, convertBack(values));
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
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
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
        return new TermsQueryBuilder(fieldName, values, termsLookup)
                .boost(boost)
                .queryName(queryName);
    }

    static List<Object> parseValues(XContentParser parser) throws IOException {
        List<Object> values = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            Object value = parser.objectBytes();
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
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (termsLookup != null || supplier != null) {
            throw new UnsupportedOperationException("query must be rewritten first");
        }
        if (values == null || values.isEmpty()) {
            return Queries.newMatchNoDocsQuery("No terms supplied for \"" + getName() + "\" query.");
        }
        MappedFieldType fieldType = context.fieldMapper(fieldName);

        if (fieldType != null) {
            return fieldType.termsQuery(values, context);
        } else {
            BytesRef[] filterValues = new BytesRef[values.size()];
            for (int i = 0; i < filterValues.length; i++) {
                filterValues[i] = BytesRefs.toBytesRef(values.get(i));
            }
            return new TermInSetQuery(fieldName, filterValues);
        }
    }

    private void fetch(TermsLookup termsLookup, Client client, ActionListener<List<Object>> actionListener) {
        GetRequest getRequest = new GetRequest(termsLookup.index(), termsLookup.type(), termsLookup.id())
            .preference("_local").routing(termsLookup.routing());
        client.get(getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                List<Object> terms = new ArrayList<>();
                if (getResponse.isSourceEmpty() == false) { // extract terms only if the doc source exists
                    List<Object> extractedValues = XContentMapValues.extractRawValues(termsLookup.path(), getResponse.getSourceAsMap());
                    terms.addAll(extractedValues);
                }
                actionListener.onResponse(terms);
            }

            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        });
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, values, termsLookup, supplier);
    }

    @Override
    protected boolean doEquals(TermsQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
                Objects.equals(values, other.values) &&
                Objects.equals(termsLookup, other.termsLookup) &&
                Objects.equals(supplier, other.supplier);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        if (supplier != null) {
            return supplier.get() == null ? this : new TermsQueryBuilder(this.fieldName, supplier.get());
        } else if (this.termsLookup != null) {
            SetOnce<List<?>> supplier = new SetOnce<>();
            queryRewriteContext.registerAsyncAction((client, listener) -> {
                fetch(termsLookup, client, ActionListener.wrap(list -> {
                    supplier.set(list);
                    listener.onResponse(null);
                }, listener::onFailure));

            });
            return new TermsQueryBuilder(this.fieldName, supplier::get);
        }
        return this;
    }
}
