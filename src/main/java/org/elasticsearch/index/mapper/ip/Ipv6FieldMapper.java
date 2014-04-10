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

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.BigNumericRangeFilter;
import org.apache.lucene.search.BigNumericRangeQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.*;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.io.Reader;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class Ipv6FieldMapper extends NumberFieldMapper<BigInteger> {

    public static final String CONTENT_TYPE = "ipv6";

    private static final int BITS = 129;


    public static BigInteger ipv6ToNumber(String addr) {
        int startIndex=addr.indexOf("::");

        if(startIndex!=-1){


            String firstStr=addr.substring(0,startIndex);
            String secondStr=addr.substring(startIndex+2, addr.length());


            BigInteger first=ipv6ToNumber(firstStr);

            int x=countChar(addr, ':');

            first=first.shiftLeft(16*(7-x)).add(ipv6ToNumber(secondStr));

            return first;
        }


        String[] strArr = addr.split(":");

        BigInteger retValue = BigInteger.valueOf(0);
        for (int i=0;i<strArr.length;i++) {
            BigInteger bi=new BigInteger(strArr[i], 16);
            retValue = retValue.shiftLeft(16).add(bi);
        }
        return retValue;
    }


    public static String numberToIPv6(BigInteger ipNumber) {
        String ipString ="";
        BigInteger a =new BigInteger("FFFF", 16);

        for (int i=0; i<8; i++) {
            ipString=ipNumber.and(a).toString(16)+":"+ipString;

            ipNumber = ipNumber.shiftRight(16);
        }

        return ipString.substring(0, ipString.length()-1);

    }

    private static int countChar(String str, char reg){
        char[] ch=str.toCharArray();
        int count=0;
        for(int i=0; i<ch.length; ++i){
            if(ch[i]==reg){
                if(ch[i+1]==reg){
                    ++i;
                    continue;
                }
                ++count;
            }
        }
        return count;
    }

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final String NULL_VALUE = null;

        public static final FieldType FIELD_TYPE = new FieldType(NumberFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.freeze();
        }
    }

    private ThreadLocal<BigNumericTokenStream> tokenStream = new ThreadLocal<BigNumericTokenStream>() {
        @Override
        protected BigNumericTokenStream initialValue() {
            return new BigNumericTokenStream(precisionStep, BITS);
        }
    };

    private static ThreadLocal<BigNumericTokenStream> tokenStream4 = new ThreadLocal<BigNumericTokenStream>() {
        @Override
        protected BigNumericTokenStream initialValue() {
            return new BigNumericTokenStream(4, BITS);
        }
    };

    private static ThreadLocal<BigNumericTokenStream> tokenStream8 = new ThreadLocal<BigNumericTokenStream>() {
        @Override
        protected BigNumericTokenStream initialValue() {
            return new BigNumericTokenStream(8, BITS);
        }
    };

    private static ThreadLocal<BigNumericTokenStream> tokenStreamMax = new ThreadLocal<BigNumericTokenStream>() {
        @Override
        protected BigNumericTokenStream initialValue() {
            return new BigNumericTokenStream(Integer.MAX_VALUE, BITS);
        }
    };


    protected BigNumericTokenStream popCachedBigStream() {
        if (precisionStep == 4) {
            return tokenStream4.get();
        }
        if (precisionStep == 8) {
            return tokenStream8.get();
        }
        if (precisionStep == Integer.MAX_VALUE) {
            return tokenStreamMax.get();
        }
        return tokenStream.get();
    }


    public static class Builder extends NumberFieldMapper.Builder<Builder, Ipv6FieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
            builder = this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public Ipv6FieldMapper build(BuilderContext context) {
            fieldType.setOmitNorms(fieldType.omitNorms() && boost == 1.0f);
            Ipv6FieldMapper fieldMapper = new Ipv6FieldMapper(buildNames(context),
                    precisionStep, boost, fieldType, docValues, nullValue, ignoreMalformed(context), coerce(context),
                    postingsProvider, docValuesProvider, similarity,
                    normsLoading, fieldDataSettings, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Ipv6FieldMapper.Builder builder = new Ipv6FieldMapper.Builder(name);
            parseNumberField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    builder.nullValue(propNode.toString());
                }
            }
            return builder;
        }
    }

    private String nullValue;

    protected Ipv6FieldMapper(Names names, int precisionStep, float boost, FieldType fieldType, Boolean docValues,
                              String nullValue, Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                              PostingsFormatProvider postingsProvider, DocValuesFormatProvider docValuesProvider,
                              SimilarityProvider similarity, Loading normsLoading, @Nullable Settings fieldDataSettings,
                              Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(names, precisionStep, boost, fieldType, docValues,
                ignoreMalformed, coerce, new NamedAnalyzer("_ipv6/" + precisionStep, new NumericIpv6Analyzer(precisionStep)),
                new NamedAnalyzer("_ipv6/max", new NumericIpv6Analyzer(Integer.MAX_VALUE)), postingsProvider, docValuesProvider,
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
        return BITS;
    }

    @Override
    public BigInteger value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        }
        if (value instanceof BytesRef) {
            return new BigInteger(((BytesRef) value).bytes);
        }
        return ipv6ToNumber(value.toString());
    }

    /**
     * IPs should return as a string.
     */
    @Override
    public Object valueForSearch(Object value) {
        BigInteger val = value(value);
        if (val == null) {
            return null;
        }
        return numberToIPv6(val);
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        BytesRef bytesRef = new BytesRef();
        BigNumericUtils.bigIntToPrefixCodedBytes(parseValue(value), 0, bytesRef, BITS); // 0 because of exact match
        return bytesRef;
    }

    private BigInteger parseValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        }
        if (value instanceof BytesRef) {
            return ipv6ToNumber(((BytesRef) value).utf8ToString());
        }
        return ipv6ToNumber(value.toString());
    }

    @Override
    public Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        BigInteger iValue = ipv6ToNumber(value);
        BigInteger iSim = ipv6ToNumber(fuzziness.asString());
        return BigNumericRangeQuery.newBigIntegerRange(names.indexName(), precisionStep,
                iValue.subtract(iSim),
                iValue.add(iSim),
                true, true, BITS);
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return BigNumericRangeQuery.newBigIntegerRange(names.indexName(), precisionStep,
                parseValue(lowerTerm), parseValue(upperTerm), includeLower, includeUpper, BITS);
    }

    @Override
    public Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return BigNumericRangeFilter.newBigIntegerRange(names.indexName(), precisionStep,
                parseValue(lowerTerm), parseValue(upperTerm), includeLower, includeUpper, BITS);
    }

    @Override
    public Filter rangeFilter(IndexFieldDataService fieldData, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        throw new UnsupportedOperationException();
//        return NumericRangeFieldDataFilter.newLongRange((IndexNumericFieldData) fieldData.getForField(this),
//                lowerTerm == null ? null : parseValue(lowerTerm),
//                upperTerm == null ? null : parseValue(upperTerm),
//                includeLower, includeUpper);
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        final BigInteger value = ipv6ToNumber(nullValue);
        return BigNumericRangeFilter.newBigIntegerRange(names.indexName(), precisionStep,
                value, value, true, true, BITS);
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

        final BigInteger value = ipv6ToNumber(ipAsString);
        if (fieldType.indexed() || fieldType.stored()) {
            CustomBigIntegerNumericField field = new CustomBigIntegerNumericField(this, value, fieldType);
            field.setBoost(boost);
            fields.add(field);
        }
        if (hasDocValues()) {
//            addDocValue(context, value);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeContext.mergeFlags().simulate()) {
            this.nullValue = ((Ipv6FieldMapper) mergeWith).nullValue;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || precisionStep != Defaults.PRECISION_STEP) {
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

    public static class CustomBigIntegerNumericField extends CustomNumericField {

        private final BigInteger number;

        private final Ipv6FieldMapper mapper;

        public CustomBigIntegerNumericField(Ipv6FieldMapper mapper, BigInteger number, FieldType fieldType) {
            super(mapper, number, fieldType);
            this.mapper = mapper;
            this.number = number;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer) throws IOException {
            if (fieldType().indexed()) {
                return mapper.popCachedBigStream().setBigIntValue(number);
            }
            return null;
        }

        @Override
        public String numericAsString() {
            return number.toString();
        }
    }

    public static class NumericIpv6Analyzer extends Analyzer {

        private final int precisionStep;

        public NumericIpv6Analyzer(int precisionStep) {
            this.precisionStep = precisionStep;
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
            try {
                // LUCENE 4 UPGRADE: in reusableTokenStream the buffer size was char[120]
                // Not sure if this is intentional or not
                return new TokenStreamComponents(createNumericTokenizer(reader, new char[32]));
            } catch (IOException e) {
                throw new RuntimeException("Failed to create numeric tokenizer", e);
            }
        }

        protected NumericIpv6Tokenizer createNumericTokenizer(Reader reader, char[] buffer) throws IOException {
            return new NumericIpv6Tokenizer(reader, precisionStep, buffer);
        }
    }

    public static class NumericIpv6Tokenizer extends Tokenizer {

        /** Make this tokenizer get attributes from the delegate token stream. */
        private static final AttributeSource.AttributeFactory delegatingAttributeFactory(final AttributeSource source) {
            return new AttributeFactory() {
                @Override
                public AttributeImpl createAttributeInstance(Class<? extends Attribute> attClass) {
                    return (AttributeImpl) source.addAttribute(attClass);
                }
            };
        }

        private final BigNumericTokenStream numericTokenStream;
        private final char[] buffer;
        private boolean started;

        public NumericIpv6Tokenizer(Reader reader, int precisionStep, char[] buffer) throws IOException {
            this(reader, new BigNumericTokenStream(precisionStep, BITS), buffer);
        }

        protected NumericIpv6Tokenizer(Reader reader, BigNumericTokenStream numericTokenStream, char[] buffer) throws IOException {
            super(delegatingAttributeFactory(numericTokenStream), reader);
            this.numericTokenStream = numericTokenStream;
            // Add attributes from the numeric token stream, this works fine because the attribute factory delegates to numericTokenStream
            for (Iterator<Class<? extends Attribute>> it = numericTokenStream.getAttributeClassesIterator(); it.hasNext();) {
                addAttribute(it.next());
            }
            this.buffer = buffer;
            started = true;
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            started = false;
        }

        @Override
        public final boolean incrementToken() throws IOException {
            if (!started) {
                // reset() must be idempotent, this is why we read data in incrementToken
                final int len = Streams.readFully(input, buffer);
                if (len == buffer.length && input.read() != -1) {
                    throw new IOException("Cannot read numeric data larger than " + buffer.length + " chars");
                }
                setValue(numericTokenStream, new String(buffer, 0, len));
                numericTokenStream.reset();
                started = true;
            }
            return numericTokenStream.incrementToken();
        }

        @Override
        public void end() throws IOException {
            super.end();
            numericTokenStream.end();
        }

        @Override
        public void close() throws IOException {
            super.close();
            numericTokenStream.close();
        }

        protected void setValue(BigNumericTokenStream tokenStream, String value) {
            tokenStream.setBigIntValue(ipv6ToNumber(value));
        }
    }
}
