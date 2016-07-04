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

package org.elasticsearch.index.mapper.ip;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.XPointValues;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.LegacyNumberFieldMapper.Defaults;
import org.elasticsearch.index.mapper.core.TypeParsers;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** A {@link FieldMapper} for ip addresses. */
public class IpFieldMapper extends FieldMapper implements AllFieldMapper.IncludeInAll {

    public static final String CONTENT_TYPE = "ip";

    public static class Builder extends FieldMapper.Builder<Builder, IpFieldMapper> {

        private Boolean ignoreMalformed;

        public Builder(String name) {
            super(name, new IpFieldType(), new IpFieldType());
            builder = this;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
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
            setupFieldType(context);
            IpFieldMapper fieldMapper = new IpFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context),
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            return (IpFieldMapper) fieldMapper.includeInAll(includeInAll);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        public TypeParser() {
        }

        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (parserContext.indexVersionCreated().before(Version.V_5_0_0_alpha2)) {
                return new LegacyIpFieldMapper.TypeParser().parse(name, node, parserContext);
            }
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
                    builder.ignoreMalformed(TypeParsers.nodeBooleanValue("ignore_malformed", propNode, parserContext));
                    iterator.remove();
                } else if (TypeParsers.parseMultiField(builder, name, parserContext, propName, propNode)) {
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class IpFieldType extends MappedFieldType {

        IpFieldType() {
            super();
            setTokenized(false);
            setHasDocValues(true);
        }

        IpFieldType(IpFieldType other) {
            super(other);
        }

        @Override
        public MappedFieldType clone() {
            return new IpFieldType(this);
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
                    String[] fields = term.split("/");
                    if (fields.length == 2) {
                        InetAddress address = InetAddresses.forString(fields[0]);
                        int prefixLength = Integer.parseInt(fields[1]);
                        return InetAddressPoint.newPrefixQuery(name(), address, prefixLength);
                    } else {
                        throw new IllegalArgumentException("Expected [ip/prefix] but was [" + term + "]");
                    }
                }
                InetAddress address = InetAddresses.forString(term);
                return InetAddressPoint.newExactQuery(name(), address);
            }
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper) {
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

        @Override
        public FieldStats.Ip stats(IndexReader reader) throws IOException {
            String field = name();
            long size = XPointValues.size(reader, field);
            if (size == 0) {
                return null;
            }
            int docCount = XPointValues.getDocCount(reader, field);
            byte[] min = XPointValues.getMinPackedValue(reader, field);
            byte[] max = XPointValues.getMaxPackedValue(reader, field);
            return new FieldStats.Ip(reader.maxDoc(), docCount, -1L, size,
                isSearchable(), isAggregatable(),
                InetAddressPoint.decode(min), InetAddressPoint.decode(max));
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder();
        }

        @Override
        public Object valueForSearch(Object value) {
            if (value == null) {
                return null;
            }
            return DocValueFormat.IP.format((BytesRef) value);
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, DateTimeZone timeZone) {
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

    private Boolean includeInAll;

    private Explicit<Boolean> ignoreMalformed;

    private IpFieldMapper(
            String simpleName,
            MappedFieldType fieldType,
            MappedFieldType defaultFieldType,
            Explicit<Boolean> ignoreMalformed,
            Settings indexSettings,
            MultiFields multiFields,
            CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
    }

    @Override
    public IpFieldType fieldType() {
        return (IpFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType.typeName();
    }

    @Override
    protected IpFieldMapper clone() {
        return (IpFieldMapper) super.clone();
    }

    @Override
    public Mapper includeInAll(Boolean includeInAll) {
        if (includeInAll != null) {
            IpFieldMapper clone = clone();
            clone.includeInAll = includeInAll;
            return clone;
        } else {
            return this;
        }
    }

    @Override
    public Mapper includeInAllIfNotSet(Boolean includeInAll) {
        if (includeInAll != null && this.includeInAll == null) {
            IpFieldMapper clone = clone();
            clone.includeInAll = includeInAll;
            return clone;
        } else {
            return this;
        }
    }

    @Override
    public Mapper unsetIncludeInAll() {
        if (includeInAll != null) {
            IpFieldMapper clone = clone();
            clone.includeInAll = null;
            return clone;
        } else {
            return this;
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        Object addressAsObject;
        if (context.externalValueSet()) {
            addressAsObject = context.externalValue();
        } else {
            addressAsObject = context.parser().textOrNull();
        }

        if (addressAsObject == null) {
            addressAsObject = fieldType().nullValue();
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
                    return;
                } else {
                    throw e;
                }
            }
        }

        if (context.includeInAll(includeInAll, this)) {
            context.allEntries().addText(fieldType().name(), addressAsString, fieldType().boost());
        }

        if (fieldType().indexOptions() != IndexOptions.NONE) {
            fields.add(new InetAddressPoint(fieldType().name(), address));
        }
        if (fieldType().hasDocValues()) {
            fields.add(new SortedSetDocValuesField(fieldType().name(), new BytesRef(InetAddressPoint.encode(address))));
        }
        if (fieldType().stored()) {
            fields.add(new StoredField(fieldType().name(), new BytesRef(InetAddressPoint.encode(address))));
        }
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        IpFieldMapper other = (IpFieldMapper) mergeWith;
        this.includeInAll = other.includeInAll;
        if (other.ignoreMalformed.explicit()) {
            this.ignoreMalformed = other.ignoreMalformed;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || fieldType().nullValue() != null) {
            Object nullValue = fieldType().nullValue();
            if (nullValue != null) {
                nullValue = InetAddresses.toAddrString((InetAddress) nullValue);
            }
            builder.field("null_value", nullValue);
        }

        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field("ignore_malformed", ignoreMalformed.value());
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", false);
        }
    }
}
