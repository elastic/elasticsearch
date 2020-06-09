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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.net.InetAddress;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** A {@link FieldMapper} for ip addresses. */
public class IpFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "ip";

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final FieldType FIELD_TYPE = new FieldType();
        static {
            FIELD_TYPE.setDimensions(1, Integer.BYTES);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        private Boolean ignoreMalformed;
        private InetAddress nullValue;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        public Builder nullValue(InetAddress nullValue) {
            this.nullValue = nullValue;
            return builder;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.IGNORE_MALFORMED;
        }

        @Override
        public IpFieldMapper build(BuilderContext context) {
            return new IpFieldMapper(name, fieldType, new IpFieldType(buildFullName(context), indexed, hasDocValues, meta),
                ignoreMalformed(context), nullValue,
                multiFieldsBuilder.build(this, context), copyTo);
        }

    }

    public static class TypeParser implements Mapper.TypeParser {

        public TypeParser() {
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            TypeParsers.parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(InetAddresses.forString(propNode.toString()));
                    iterator.remove();
                } else if (propName.equals("ignore_malformed")) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode, name + ".ignore_malformed"));
                    iterator.remove();
                } else if (TypeParsers.parseMultiField(builder::addMultiField, name, parserContext, propName, propNode)) {
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class IpFieldType extends SimpleMappedFieldType {

        public IpFieldType(String name, boolean indexed, boolean hasDocValues, Map<String, String> meta) {
            super(name, indexed, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
        }

        public IpFieldType(String name) {
            this(name, true, true, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        private InetAddress parse(Object value) {
            if (value instanceof InetAddress) {
                return (InetAddress) value;
            } else {
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                return InetAddresses.forString(value.toString());
            }
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            failIfNotIndexed();
            if (value instanceof InetAddress) {
                return InetAddressPoint.newExactQuery(name(), (InetAddress) value);
            } else {
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                String term = value.toString();
                if (term.contains("/")) {
                    final Tuple<InetAddress, Integer> cidr = InetAddresses.parseCidr(term);
                    return InetAddressPoint.newPrefixQuery(name(), cidr.v1(), cidr.v2());
                }
                InetAddress address = InetAddresses.forString(term);
                return InetAddressPoint.newExactQuery(name(), address);
            }
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            InetAddress[] addresses = new InetAddress[values.size()];
            int i = 0;
            for (Object value : values) {
                InetAddress address;
                if (value instanceof InetAddress) {
                    address = (InetAddress) value;
                } else {
                    if (value instanceof BytesRef) {
                        value = ((BytesRef) value).utf8ToString();
                    }
                    if (value.toString().contains("/")) {
                        // the `terms` query contains some prefix queries, so we cannot create a set query
                        // and need to fall back to a disjunction of `term` queries
                        return super.termsQuery(values, context);
                    }
                    address = InetAddresses.forString(value.toString());
                }
                addresses[i++] = address;
            }
            return InetAddressPoint.newSetQuery(name(), addresses);
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            failIfNotIndexed();
            InetAddress lower;
            if (lowerTerm == null) {
                lower = InetAddressPoint.MIN_VALUE;
            } else {
                lower = parse(lowerTerm);
                if (includeLower == false) {
                    if (lower.equals(InetAddressPoint.MAX_VALUE)) {
                        return new MatchNoDocsQuery();
                    }
                    lower = InetAddressPoint.nextUp(lower);
                }
            }

            InetAddress upper;
            if (upperTerm == null) {
                upper = InetAddressPoint.MAX_VALUE;
            } else {
                upper = parse(upperTerm);
                if (includeUpper == false) {
                    if (upper.equals(InetAddressPoint.MIN_VALUE)) {
                        return new MatchNoDocsQuery();
                    }
                    upper = InetAddressPoint.nextDown(upper);
                }
            }

            return InetAddressPoint.newRangeQuery(name(), lower, upper);
        }

        public static final class IpScriptDocValues extends ScriptDocValues<String> {

            private final SortedSetDocValues in;
            private long[] ords = new long[0];
            private int count;

            public IpScriptDocValues(SortedSetDocValues in) {
                this.in = in;
            }

            @Override
            public void setNextDocId(int docId) throws IOException {
                count = 0;
                if (in.advanceExact(docId)) {
                    for (long ord = in.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = in.nextOrd()) {
                        ords = ArrayUtil.grow(ords, count + 1);
                        ords[count++] = ord;
                    }
                }
            }

            public String getValue() {
                if (count == 0) {
                    return null;
                } else {
                    return get(0);
                }
            }

            @Override
            public String get(int index) {
                try {
                    BytesRef encoded = in.lookupOrd(ords[index]);
                    InetAddress address = InetAddressPoint.decode(
                            Arrays.copyOfRange(encoded.bytes, encoded.offset, encoded.offset + encoded.length));
                    return InetAddresses.toAddrString(address);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public int size() {
                return count;
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), IpScriptDocValues::new, CoreValuesSourceType.IP);
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return DocValueFormat.IP.format((BytesRef) value);
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
            }
            if (timeZone != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName()
                    + "] does not support custom time zones");
            }
            return DocValueFormat.IP;
        }
    }

    private Explicit<Boolean> ignoreMalformed;
    private final InetAddress nullValue;

    private IpFieldMapper(
            String simpleName,
            FieldType fieldType,
            MappedFieldType mappedFieldType,
            Explicit<Boolean> ignoreMalformed,
            InetAddress nullValue,
            MultiFields multiFields,
            CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.nullValue = nullValue;
    }

    @Override
    public IpFieldType fieldType() {
        return (IpFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType().typeName();
    }

    @Override
    protected IpFieldMapper clone() {
        return (IpFieldMapper) super.clone();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        Object addressAsObject;
        if (context.externalValueSet()) {
            addressAsObject = context.externalValue();
        } else {
            addressAsObject = context.parser().textOrNull();
        }

        if (addressAsObject == null) {
            addressAsObject = nullValue;
        }

        if (addressAsObject == null) {
            return;
        }

        String addressAsString = addressAsObject.toString();
        InetAddress address;
        if (addressAsObject instanceof InetAddress) {
            address = (InetAddress) addressAsObject;
        } else {
            try {
                address = InetAddresses.forString(addressAsString);
            } catch (IllegalArgumentException e) {
                if (ignoreMalformed.value()) {
                    context.addIgnoredField(fieldType().name());
                    return;
                } else {
                    throw e;
                }
            }
        }

        if (fieldType().isSearchable()) {
            context.doc().add(new InetAddressPoint(fieldType().name(), address));
        }
        if (fieldType().hasDocValues()) {
            context.doc().add(new SortedSetDocValuesField(fieldType().name(), new BytesRef(InetAddressPoint.encode(address))));
        } else if (fieldType.stored() || fieldType().isSearchable()) {
            createFieldNamesField(context);
        }
        if (fieldType.stored()) {
            context.doc().add(new StoredField(fieldType().name(), new BytesRef(InetAddressPoint.encode(address))));
        }
    }

    @Override
    protected String parseSourceValue(Object value, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
        }

        InetAddress address = InetAddresses.forString(value.toString());
        return InetAddresses.toAddrString(address);
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        IpFieldMapper mergeWith = (IpFieldMapper) other;
        if (mergeWith.nullValue != this.nullValue) {
            conflicts.add("mapper [" + name() + "] has different [null_value] values");
        }
        if (mergeWith.ignoreMalformed.explicit()) {
            this.ignoreMalformed = mergeWith.ignoreMalformed;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (nullValue != null) {
            builder.field("null_value", InetAddresses.toAddrString(nullValue));
        }

        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field("ignore_malformed", ignoreMalformed.value());
        }
    }
}
