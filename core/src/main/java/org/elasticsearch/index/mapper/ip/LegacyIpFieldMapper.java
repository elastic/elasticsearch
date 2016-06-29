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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.LegacyNumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LegacyNumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.network.Cidrs;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.LegacyLongFieldMapper;
import org.elasticsearch.index.mapper.core.LegacyLongFieldMapper.CustomLongNumericField;
import org.elasticsearch.index.mapper.core.LegacyNumberFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class LegacyIpFieldMapper extends LegacyNumberFieldMapper {

    public static final String CONTENT_TYPE = "ip";
    public static final long MAX_IP = 4294967296L;

    public static String longToIp(long longIp) {
        int octet3 = (int) ((longIp >> 24) % 256);
        int octet2 = (int) ((longIp >> 16) % 256);
        int octet1 = (int) ((longIp >> 8) % 256);
        int octet0 = (int) ((longIp) % 256);
        return octet3 + "." + octet2 + "." + octet1 + "." + octet0;
    }

    private static final Pattern pattern = Pattern.compile("\\.");

    public static long ipToLong(String ip) {
        try {
            if (!InetAddresses.isInetAddress(ip)) {
                throw new IllegalArgumentException("failed to parse ip [" + ip + "], not a valid ip address");
            }
            String[] octets = pattern.split(ip);
            if (octets.length != 4) {
                throw new IllegalArgumentException("failed to parse ip [" + ip + "], not a valid ipv4 address (4 dots)");
            }
            return (Long.parseLong(octets[0]) << 24) + (Integer.parseInt(octets[1]) << 16) +
                    (Integer.parseInt(octets[2]) << 8) + Integer.parseInt(octets[3]);
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) e;
            }
            throw new IllegalArgumentException("failed to parse ip [" + ip + "]", e);
        }
    }

    public static class Defaults extends LegacyNumberFieldMapper.Defaults {
        public static final String NULL_VALUE = null;

        public static final MappedFieldType FIELD_TYPE = new IpFieldType();

        static {
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends LegacyNumberFieldMapper.Builder<Builder, LegacyIpFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.PRECISION_STEP_64_BIT);
            builder = this;
        }

        @Override
        public LegacyIpFieldMapper build(BuilderContext context) {
            if (context.indexCreatedVersion().onOrAfter(Version.V_5_0_0_alpha2)) {
                throw new IllegalStateException("Cannot use legacy numeric types after 5.0");
            }
            setupFieldType(context);
            LegacyIpFieldMapper fieldMapper = new LegacyIpFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context),
                    coerce(context), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            return (LegacyIpFieldMapper) fieldMapper.includeInAll(includeInAll);
        }

        @Override
        protected int maxPrecisionStep() {
            return 64;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            LegacyIpFieldMapper.Builder builder = new Builder(name);
            parseNumberField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(propNode.toString());
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class IpFieldType extends LegacyLongFieldMapper.LongFieldType {

        public IpFieldType() {
        }

        protected IpFieldType(IpFieldType ref) {
            super(ref);
        }

        @Override
        public NumberFieldType clone() {
            return new IpFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        /**
         * IPs should return as a string.
         */
        @Override
        public Object valueForSearch(Object value) {
            Long val = (Long) value;
            if (val == null) {
                return null;
            }
            return longToIp(val);
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            BytesRefBuilder bytesRef = new BytesRefBuilder();
            LegacyNumericUtils.longToPrefixCoded(parseValue(value), 0, bytesRef); // 0 because of exact match
            return bytesRef.get();
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            if (value != null) {
                String term;
                if (value instanceof BytesRef) {
                    term = ((BytesRef) value).utf8ToString();
                } else {
                    term = value.toString();
                }
                long[] fromTo;
                // assume that the term is either a CIDR range or the
                // term is a single IPv4 address; if either of these
                // assumptions is wrong, the CIDR parsing will fail
                // anyway, and that is okay
                if (term.contains("/")) {
                    // treat the term as if it is in CIDR notation
                    fromTo = Cidrs.cidrMaskToMinMax(term);
                } else {
                    // treat the term as if it is a single IPv4, and
                    // apply a CIDR mask equivalent to the host route
                    fromTo = Cidrs.cidrMaskToMinMax(term + "/32");
                }
                if (fromTo != null) {
                    return rangeQuery(fromTo[0] == 0 ? null : fromTo[0],
                            fromTo[1] == MAX_IP ? null : fromTo[1], true, false);
                }
            }
            return super.termQuery(value, context);
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper) {
            return LegacyNumericRangeQuery.newLongRange(name(), numericPrecisionStep(),
                lowerTerm == null ? null : parseValue(lowerTerm),
                upperTerm == null ? null : parseValue(upperTerm),
                includeLower, includeUpper);
        }

        @Override
        public FieldStats stats(IndexReader reader) throws IOException {
            int maxDoc = reader.maxDoc();
            Terms terms = org.apache.lucene.index.MultiFields.getTerms(reader, name());
            if (terms == null) {
                return null;
            }
            long minValue = LegacyNumericUtils.getMinLong(terms);
            long maxValue = LegacyNumericUtils.getMaxLong(terms);
            return new FieldStats.Ip(maxDoc, terms.getDocCount(), terms.getSumDocFreq(), terms.getSumTotalTermFreq(),
                isSearchable(), isAggregatable(),
                InetAddress.getByName(longToIp(minValue)),
                InetAddress.getByName(longToIp(maxValue)));
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            failIfNoDocValues();
            return new IndexFieldData.Builder() {
                @Override
                public IndexFieldData<?> build(IndexSettings indexSettings,
                        MappedFieldType fieldType, IndexFieldDataCache cache,
                        CircuitBreakerService breakerService, MapperService mapperService) {
                    return new LegacyIpIndexFieldData(indexSettings.getIndex(), name());
                }
            };
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

    protected LegacyIpFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                            Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                            Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, indexSettings, multiFields, copyTo);
    }

    private static long parseValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof BytesRef) {
            return ipToLong(((BytesRef) value).utf8ToString());
        }
        return ipToLong(value.toString());
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        String ipAsString;
        if (context.externalValueSet()) {
            ipAsString = (String) context.externalValue();
            if (ipAsString == null) {
                ipAsString = fieldType().nullValueAsString();
            }
        } else {
            if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
                ipAsString = fieldType().nullValueAsString();
            } else {
                ipAsString = context.parser().text();
            }
        }

        if (ipAsString == null) {
            return;
        }
        if (context.includeInAll(includeInAll, this)) {
            context.allEntries().addText(fieldType().name(), ipAsString, fieldType().boost());
        }

        final long value = ipToLong(ipAsString);
        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            CustomLongNumericField field = new CustomLongNumericField(value, fieldType());
            if (fieldType.boost() != 1f && Version.indexCreated(context.indexSettings()).before(Version.V_5_0_0_alpha1)) {
                field.setBoost(fieldType().boost());
            }
            fields.add(field);
        }
        if (fieldType().hasDocValues()) {
            addDocValue(context, fields, value);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || fieldType().numericPrecisionStep() != Defaults.PRECISION_STEP_64_BIT) {
            builder.field("precision_step", fieldType().numericPrecisionStep());
        }
        if (includeDefaults || fieldType().nullValueAsString() != null) {
            builder.field("null_value", fieldType().nullValueAsString());
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", false);
        }

    }

}
