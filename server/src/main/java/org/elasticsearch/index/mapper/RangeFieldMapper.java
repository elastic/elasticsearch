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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANYDa
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.index.query.RangeQueryBuilder.GTE_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.GT_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.LTE_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.LT_FIELD;

/** A {@link FieldMapper} for indexing numeric and date ranges, and creating queries */
public class RangeFieldMapper extends FieldMapper {
    public static final boolean DEFAULT_INCLUDE_UPPER = true;
    public static final boolean DEFAULT_INCLUDE_LOWER = true;

    public static class Defaults {
        public static final Explicit<Boolean> COERCE = new Explicit<>(true, false);
    }

    // this is private since it has a different default
    static final Setting<Boolean> COERCE_SETTING =
        Setting.boolSetting("index.mapping.coerce", true, Setting.Property.IndexScope);

    public static class Builder extends FieldMapper.Builder<Builder, RangeFieldMapper> {
        private Boolean coerce;
        private Locale locale = Locale.ROOT;
        private String pattern;

        public Builder(String name, RangeType type) {
            super(name, new RangeFieldType(type), new RangeFieldType(type));
            builder = this;
        }

        @Override
        public RangeFieldType fieldType() {
            return (RangeFieldType)fieldType;
        }

        @Override
        public Builder docValues(boolean docValues) {
            if (docValues == true) {
                throw new IllegalArgumentException("field [" + name + "] does not currently support " + TypeParsers.DOC_VALUES);
            }
            return super.docValues(docValues);
        }

        public Builder coerce(boolean coerce) {
            this.coerce = coerce;
            return builder;
        }

        protected Explicit<Boolean> coerce(BuilderContext context) {
            if (coerce != null) {
                return new Explicit<>(coerce, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(COERCE_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.COERCE;
        }

        public Builder format(String format) {
            this.pattern = format;
            return this;
        }

        @Override
        public Builder nullValue(Object nullValue) {
            throw new IllegalArgumentException("Field [" + name() + "] does not support null value.");
        }

        public void locale(Locale locale) {
            this.locale = locale;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            DateFormatter formatter = fieldType().dateTimeFormatter;
            if (fieldType().rangeType == RangeType.DATE) {
                boolean hasPatternChanged = Strings.hasLength(builder.pattern) &&
                    Objects.equals(builder.pattern, formatter.pattern()) == false;

                if (hasPatternChanged || Objects.equals(builder.locale, formatter.locale()) == false) {
                    fieldType().setDateTimeFormatter(DateFormatter.forPattern(pattern).withLocale(locale));
                }
            } else if (pattern != null) {
                throw new IllegalArgumentException("field [" + name() + "] of type [" + fieldType().rangeType
                    + "] should not define a dateTimeFormatter unless it is a " + RangeType.DATE + " type");
            }
        }

        @Override
        public RangeFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new RangeFieldMapper(name, fieldType, defaultFieldType, coerce(context),
                context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        final RangeType type;

        public TypeParser(RangeType type) {
            this.type = type;
        }

        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node,
                                         ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name, type);
            TypeParsers.parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    throw new MapperParsingException("Property [null_value] is not supported for [" + this.type.name
                            + "] field types.");
                } else if (propName.equals("coerce")) {
                    builder.coerce(XContentMapValues.nodeBooleanValue(propNode, name + ".coerce"));
                    iterator.remove();
                } else if (propName.equals("locale")) {
                    builder.locale(LocaleUtils.parse(propNode.toString()));
                    iterator.remove();
                } else if (propName.equals("format")) {
                    builder.format(propNode.toString());
                    iterator.remove();
                } else if (TypeParsers.parseMultiField(builder, name, parserContext, propName, propNode)) {
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class RangeFieldType extends MappedFieldType {
        protected RangeType rangeType;
        protected DateFormatter dateTimeFormatter;
        protected DateMathParser dateMathParser;

        RangeFieldType(RangeType type) {
            super();
            this.rangeType = Objects.requireNonNull(type);
            setTokenized(false);
            setHasDocValues(true);
            setOmitNorms(true);
            if (rangeType == RangeType.DATE) {
                setDateTimeFormatter(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER);
            }
        }

        RangeFieldType(RangeFieldType other) {
            super(other);
            this.rangeType = other.rangeType;
            if (other.rangeType == RangeType.DATE && other.dateTimeFormatter() != null) {
                setDateTimeFormatter(other.dateTimeFormatter());
            }
        }

        public RangeType rangeType() { return rangeType; }

        @Override
        public MappedFieldType clone() {
            return new RangeFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            RangeFieldType that = (RangeFieldType) o;
            return Objects.equals(rangeType, that.rangeType) &&
            (rangeType == RangeType.DATE) ?
                Objects.equals(dateTimeFormatter, that.dateTimeFormatter)
                : dateTimeFormatter == null && that.dateTimeFormatter == null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), rangeType, dateTimeFormatter);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder().setRangeType(rangeType);
        }

        @Override
        public String typeName() {
            return rangeType.name;
        }

        public DateFormatter dateTimeFormatter() {
            return dateTimeFormatter;
        }

        public void setDateTimeFormatter(DateFormatter dateTimeFormatter) {
            checkIfFrozen();
            this.dateTimeFormatter = dateTimeFormatter;
            this.dateMathParser = dateTimeFormatter.toDateMathParser();
        }

        protected DateMathParser dateMathParser() {
            return dateMathParser;
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
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            if (rangeType == RangeType.DATE) {
                DateFormatter dateTimeFormatter = this.dateTimeFormatter;
                if (format != null) {
                    dateTimeFormatter = DateFormatter.forPattern(format).withLocale(dateTimeFormatter.locale());
                }
                if (timeZone == null) {
                    timeZone = ZoneOffset.UTC;
                }
                // the resolution here is always set to milliseconds, as aggregations use this formatter mainly and those are always in
                // milliseconds. The only special case here is docvalue fields, which are handled somewhere else
                return new DocValueFormat.DateTime(dateTimeFormatter, timeZone, DateFieldMapper.Resolution.MILLISECONDS);
            }
            return super.docValueFormat(format, timeZone);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            Query query = rangeQuery(value, value, true, true, ShapeRelation.INTERSECTS, null, null, context);
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper,
                                ShapeRelation relation, ZoneId timeZone, DateMathParser parser, QueryShardContext context) {
            failIfNotIndexed();
            if (parser == null) {
                parser = dateMathParser();
            }
            return rangeType.rangeQuery(name(), hasDocValues(), lowerTerm, upperTerm, includeLower, includeUpper, relation,
                timeZone, parser, context);
        }
    }

    private Explicit<Boolean> coerce;

    private RangeFieldMapper(
        String simpleName,
        MappedFieldType fieldType,
        MappedFieldType defaultFieldType,
        Explicit<Boolean> coerce,
        Settings indexSettings,
        MultiFields multiFields,
        CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.coerce = coerce;
    }

    @Override
    public RangeFieldType fieldType() {
        return (RangeFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType.typeName();
    }

    @Override
    protected RangeFieldMapper clone() {
        return (RangeFieldMapper) super.clone();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        Range range;
        if (context.externalValueSet()) {
            range = context.parseExternalValue(Range.class);
        } else {
            XContentParser parser = context.parser();
            final XContentParser.Token start = parser.currentToken();
            if (start == XContentParser.Token.VALUE_NULL) {
                return;
            } else if (start == XContentParser.Token.START_OBJECT) {
                RangeFieldType fieldType = fieldType();
                RangeType rangeType = fieldType.rangeType;
                String fieldName = null;
                Object from = rangeType.minValue();
                Object to = rangeType.maxValue();
                boolean includeFrom = DEFAULT_INCLUDE_LOWER;
                boolean includeTo = DEFAULT_INCLUDE_UPPER;
                XContentParser.Token token;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                    } else {
                        if (fieldName.equals(GT_FIELD.getPreferredName())) {
                            includeFrom = false;
                            if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                from = rangeType.parseFrom(fieldType, parser, coerce.value(), includeFrom);
                            }
                        } else if (fieldName.equals(GTE_FIELD.getPreferredName())) {
                            includeFrom = true;
                            if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                from = rangeType.parseFrom(fieldType, parser, coerce.value(), includeFrom);
                            }
                        } else if (fieldName.equals(LT_FIELD.getPreferredName())) {
                            includeTo = false;
                            if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                to = rangeType.parseTo(fieldType, parser, coerce.value(), includeTo);
                            }
                        } else if (fieldName.equals(LTE_FIELD.getPreferredName())) {
                            includeTo = true;
                            if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                to = rangeType.parseTo(fieldType, parser, coerce.value(), includeTo);
                            }
                        } else {
                            throw new MapperParsingException("error parsing field [" +
                                name() + "], with unknown parameter [" + fieldName + "]");
                        }
                    }
                }
                range = new Range(rangeType, from, to, includeFrom, includeTo);
            } else if (fieldType().rangeType == RangeType.IP && start == XContentParser.Token.VALUE_STRING) {
                range = parseIpRangeFromCidr(parser);
            } else {
                throw new MapperParsingException("error parsing field ["
                    + name() + "], expected an object but got " + parser.currentName());
            }
        }
        boolean indexed = fieldType.indexOptions() != IndexOptions.NONE;
        boolean docValued = fieldType.hasDocValues();
        boolean stored = fieldType.stored();
        fields.addAll(fieldType().rangeType.createFields(context, name(), range, indexed, docValued, stored));
        if (docValued == false && (indexed || stored)) {
            createFieldNamesField(context, fields);
        }
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
        RangeFieldMapper other = (RangeFieldMapper) mergeWith;
        if (other.coerce.explicit()) {
            this.coerce = other.coerce;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (fieldType().rangeType == RangeType.DATE
                && (includeDefaults || (fieldType().dateTimeFormatter() != null
                && fieldType().dateTimeFormatter().pattern().equals(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.pattern()) == false))) {
            builder.field("format", fieldType().dateTimeFormatter().pattern());
        }
        if (fieldType().rangeType == RangeType.DATE
                && (includeDefaults || (fieldType().dateTimeFormatter() != null
                && fieldType().dateTimeFormatter().locale() != Locale.ROOT))) {
            builder.field("locale", fieldType().dateTimeFormatter().locale());
        }
        if (includeDefaults || coerce.explicit()) {
            builder.field("coerce", coerce.value());
        }
    }

    private static Range parseIpRangeFromCidr(final XContentParser parser) throws IOException {
        final Tuple<InetAddress, Integer> cidr = InetAddresses.parseCidr(parser.text());
        // create the lower value by zeroing out the host portion, upper value by filling it with all ones.
        byte[] lower = cidr.v1().getAddress();
        byte[] upper = lower.clone();
        for (int i = cidr.v2(); i < 8 * lower.length; i++) {
            int m = 1 << 7 - (i & 7);
            lower[i >> 3] &= ~m;
            upper[i >> 3] |= m;
        }
        try {
            return new Range(RangeType.IP, InetAddress.getByAddress(lower), InetAddress.getByAddress(upper), true, true);
        } catch (UnknownHostException bogus) {
            throw new AssertionError(bogus);
        }
    }

    /** Class defining a range */
    public static class Range {
        RangeType type;
        Object from;
        Object to;
        private boolean includeFrom;
        private boolean includeTo;

        public Range(RangeType type, Object from, Object to, boolean includeFrom, boolean includeTo) {
            this.type = type;
            this.from = from;
            this.to = to;
            this.includeFrom = includeFrom;
            this.includeTo = includeTo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Range range = (Range) o;
            return includeFrom == range.includeFrom &&
                includeTo == range.includeTo &&
                type == range.type &&
                from.equals(range.from) &&
                to.equals(range.to);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, from, to, includeFrom, includeTo);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(includeFrom ? '[' : '(');
            Object f = includeFrom || from.equals(type.minValue()) ? from : type.nextDown(from);
            Object t = includeTo || to.equals(type.maxValue()) ? to : type.nextUp(to);
            sb.append(type == RangeType.IP ? InetAddresses.toAddrString((InetAddress)f) : f.toString());
            sb.append(" : ");
            sb.append(type == RangeType.IP ? InetAddresses.toAddrString((InetAddress)t) : t.toString());
            sb.append(includeTo ? ']' : ')');
            return sb.toString();
        }

        public Object getFrom() {
            return from;
        }

        public Object getTo() {
            return to;
        }
    }

    static class BinaryRangesDocValuesField extends CustomDocValuesField {

        private final Set<Range> ranges;
        private final RangeType rangeType;

        BinaryRangesDocValuesField(String name, Range range, RangeType rangeType) {
            super(name);
            this.rangeType = rangeType;
            ranges = new HashSet<>();
            add(range);
        }

        void add(Range range) {
            ranges.add(range);
        }

        @Override
        public BytesRef binaryValue() {
            try {
                return rangeType.encodeRanges(ranges);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to encode ranges", e);
            }
        }
    }
}
