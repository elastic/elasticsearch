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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericAnalyzer;
import org.elasticsearch.index.analysis.NumericTokenizer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.LongFieldMapper.CustomLongNumericField;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.NumericRangeFieldDataFilter;
import org.elasticsearch.index.similarity.SimilarityProvider;

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
public class IpFieldMapper extends NumberFieldMapper<Long> {

    public static final String CONTENT_TYPE = "ip";

    public static String longToIp(long longIp) {
        int octet3 = (int) ((longIp >> 24) % 256);
        int octet2 = (int) ((longIp >> 16) % 256);
        int octet1 = (int) ((longIp >> 8) % 256);
        int octet0 = (int) ((longIp) % 256);
        return octet3 + "." + octet2 + "." + octet1 + "." + octet0;
    }

    private static final Pattern pattern = Pattern.compile("\\.");

    public static long ipToLong(String ip) throws ElasticsearchIllegalArgumentException {
        try {
            if (!InetAddresses.isInetAddress(ip)) {
                throw new ElasticsearchIllegalArgumentException("failed to parse ip [" + ip + "], not a valid ip address");
            }
            String[] octets = pattern.split(ip);
            if (octets.length != 4) {
                throw new ElasticsearchIllegalArgumentException("failed to parse ip [" + ip + "], not a valid ipv4 address (4 dots)");
            }
            return (Long.parseLong(octets[0]) << 24) + (Integer.parseInt(octets[1]) << 16) +
                    (Integer.parseInt(octets[2]) << 8) + Integer.parseInt(octets[3]);
        } catch (Exception e) {
            if (e instanceof ElasticsearchIllegalArgumentException) {
                throw (ElasticsearchIllegalArgumentException) e;
            }
            throw new ElasticsearchIllegalArgumentException("failed to parse ip [" + ip + "]", e);
        }
    }

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final String NULL_VALUE = null;

        public static final FieldType FIELD_TYPE = new FieldType(NumberFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, IpFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE), Defaults.PRECISION_STEP_64_BIT);
            builder = this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public IpFieldMapper build(BuilderContext context) {
            fieldType.setOmitNorms(fieldType.omitNorms() && boost == 1.0f);
            IpFieldMapper fieldMapper = new IpFieldMapper(buildNames(context),
                    fieldType.numericPrecisionStep(), boost, fieldType, docValues, nullValue, ignoreMalformed(context), coerce(context),
                    similarity, normsLoading, fieldDataSettings, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
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

    private String nullValue;

    protected IpFieldMapper(Names names, int precisionStep, float boost, FieldType fieldType, Boolean docValues,
                            String nullValue, Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                            SimilarityProvider similarity, Loading normsLoading, @Nullable Settings fieldDataSettings, 
                            Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(names, precisionStep, boost, fieldType, docValues,
                ignoreMalformed, coerce, new NamedAnalyzer("_ip/" + precisionStep, new NumericIpAnalyzer(precisionStep)),
                new NamedAnalyzer("_ip/max", new NumericIpAnalyzer(Integer.MAX_VALUE)),
                similarity, normsLoading, fieldDataSettings, indexSettings, multiFields, copyTo);
        this.nullValue = nullValue;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("long");
    }

    @Override
    protected int maxPrecisionStep() {
        return 64;
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

    private long parseValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof BytesRef) {
            return ipToLong(((BytesRef) value).utf8ToString());
        }
        return ipToLong(value.toString());
    }

    @Override
    public Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        long iValue = ipToLong(value);
        long iSim;
        try {
            iSim = ipToLong(fuzziness.asString());
        } catch (ElasticsearchIllegalArgumentException e) {
            iSim = fuzziness.asLong();
        }
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                iValue - iSim,
                iValue + iSim,
                true, true);
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : parseValue(lowerTerm),
                upperTerm == null ? null : parseValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return Queries.wrap(NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : parseValue(lowerTerm),
                upperTerm == null ? null : parseValue(upperTerm),
                includeLower, includeUpper));
    }

    @Override
    public Filter rangeFilter(QueryParseContext parseContext, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeFieldDataFilter.newLongRange((IndexNumericFieldData) parseContext.getForField(this),
                lowerTerm == null ? null : parseValue(lowerTerm),
                upperTerm == null ? null : parseValue(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        final long value = ipToLong(nullValue);
        return Queries.wrap(NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                value,
                value,
                true, true));
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        String ipAsString;
        if (context.externalValueSet()) {
            ipAsString = (String) context.externalValue();
            if (ipAsString == null) {
                ipAsString = nullValue;
            }
        } else {
            if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
                ipAsString = nullValue;
            } else {
                ipAsString = context.parser().text();
            }
        }

        if (ipAsString == null) {
            return;
        }
        if (context.includeInAll(includeInAll, this)) {
            context.allEntries().addText(names.fullName(), ipAsString, boost);
        }

        final long value = ipToLong(ipAsString);
        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            CustomLongNumericField field = new CustomLongNumericField(this, value, fieldType);
            field.setBoost(boost);
            fields.add(field);
        }
        if (hasDocValues()) {
            addDocValue(context, fields, value);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        super.merge(mergeWith, mergeResult);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeResult.simulate()) {
            this.nullValue = ((IpFieldMapper) mergeWith).nullValue;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || precisionStep != Defaults.PRECISION_STEP_64_BIT) {
            builder.field("precision_step", precisionStep);
        }
        if (includeDefaults || nullValue != null) {
            builder.field("null_value", nullValue);
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
