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

import com.google.common.net.InetAddresses;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericAnalyzer;
import org.elasticsearch.index.analysis.NumericTokenizer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper.CustomLongNumericField;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.index.mapper.MapperBuilders.ipField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class IpFieldMapper extends NumberFieldMapper {

    public static final String CONTENT_TYPE = "ip";

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

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final String NULL_VALUE = null;

        public static final MappedFieldType FIELD_TYPE = new IpFieldType();

        static {
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, IpFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.PRECISION_STEP_64_BIT);
            builder = this;
        }

        @Override
        public IpFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            IpFieldMapper fieldMapper = new IpFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context), coerce(context),
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }

        @Override
        protected NamedAnalyzer makeNumberAnalyzer(int precisionStep) {
            String name = precisionStep == Integer.MAX_VALUE ? "_ip/max" : ("_ip/" + precisionStep);
            return new NamedAnalyzer(name, new NumericIpAnalyzer(precisionStep));
        }

        @Override
        protected int maxPrecisionStep() {
            return 64;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            IpFieldMapper.Builder builder = ipField(name);
            parseNumberField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = Strings.toUnderscoreCase(entry.getKey());
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

    public static final class IpFieldType extends LongFieldMapper.LongFieldType {

        public IpFieldType() {
            setFieldDataType(new FieldDataType("long"));
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

        @Override
        public Long value(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            if (value instanceof BytesRef) {
                return Numbers.bytesToLong((BytesRef) value);
            }
            return ipToLong(value.toString());
        }

        /**
         * IPs should return as a string.
         */
        @Override
        public Object valueForSearch(Object value) {
            Long val = value(value);
            if (val == null) {
                return null;
            }
            return longToIp(val);
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            BytesRefBuilder bytesRef = new BytesRefBuilder();
            NumericUtils.longToPrefixCoded(parseValue(value), 0, bytesRef); // 0 because of exact match
            return bytesRef.get();
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper) {
            return NumericRangeQuery.newLongRange(names().indexName(), numericPrecisionStep(),
                lowerTerm == null ? null : parseValue(lowerTerm),
                upperTerm == null ? null : parseValue(upperTerm),
                includeLower, includeUpper);
        }

        @Override
        public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
            long iValue = parseValue(value);
            long iSim;
            try {
                iSim = ipToLong(fuzziness.asString());
            } catch (IllegalArgumentException e) {
                iSim = fuzziness.asLong();
            }
            return NumericRangeQuery.newLongRange(names().indexName(), numericPrecisionStep(),
                iValue - iSim,
                iValue + iSim,
                true, true);
        }
    }

    protected IpFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
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
            context.allEntries().addText(fieldType().names().fullName(), ipAsString, fieldType().boost());
        }

        final long value = ipToLong(ipAsString);
        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            CustomLongNumericField field = new CustomLongNumericField(value, fieldType());
            field.setBoost(fieldType().boost());
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

    public static class NumericIpAnalyzer extends NumericAnalyzer<NumericIpTokenizer> {

        private final int precisionStep;

        public NumericIpAnalyzer(int precisionStep) {
            this.precisionStep = precisionStep;
        }

        @Override
        protected NumericIpTokenizer createNumericTokenizer(char[] buffer) throws IOException {
            return new NumericIpTokenizer(precisionStep, buffer);
        }
    }

    public static class NumericIpTokenizer extends NumericTokenizer {

        public NumericIpTokenizer(int precisionStep, char[] buffer) throws IOException {
            super(new NumericTokenStream(precisionStep), buffer, null);
        }

        @Override
        protected void setValue(NumericTokenStream tokenStream, String value) {
            tokenStream.setLongValue(ipToLong(value));
        }
    }
}
