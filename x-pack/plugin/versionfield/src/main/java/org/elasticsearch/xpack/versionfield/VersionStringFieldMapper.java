/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TermBasedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xpack.versionfield.VersionEncoder.EncodedVersion;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.elasticsearch.xpack.versionfield.VersionEncoder.encodeVersion;

/**
 * A {@link FieldMapper} for indexing fields with version strings.
 */
public class VersionStringFieldMapper extends FieldMapper {

    private static byte[] MIN_VALUE = new byte[16];
    private static byte[] MAX_VALUE = new byte[16];
    static {
        Arrays.fill(MIN_VALUE, (byte) 0);
        Arrays.fill(MAX_VALUE, (byte) -1);
    }

    public static final String CONTENT_TYPE = "version";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }

        public static final String NULL_VALUE = null;
    }

    static class Builder extends FieldMapper.Builder<Builder> {

        protected String nullValue = Defaults.NULL_VALUE;
        private boolean storeMalformed = false;

        Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return builder;
        }

        Builder storeMalformed(boolean storeMalformed) {
            this.storeMalformed = storeMalformed;
            return builder;
        }

        private VersionStringFieldType buildFieldType(BuilderContext context) {
            boolean validateVersion = storeMalformed == false;
            return new VersionStringFieldType(buildFullName(context), indexed, validateVersion, meta, boost, fieldType);
        }

        @Override
        public VersionStringFieldMapper build(BuilderContext context) {
            BooleanFieldMapper.Builder preReleaseSubfield = new BooleanFieldMapper.Builder(name + ".isPreRelease");
            NumberType type = NumberType.INTEGER;
            NumberFieldMapper.Builder majorVersionSubField = new NumberFieldMapper.Builder(name + ".major", type).nullValue(0);
            NumberFieldMapper.Builder minorVersionSubField = new NumberFieldMapper.Builder(name + ".minor", type).nullValue(0);
            NumberFieldMapper.Builder patchVersionSubField = new NumberFieldMapper.Builder(name + ".patch", type).nullValue(0);

            return new VersionStringFieldMapper(
                name,
                fieldType,
                buildFieldType(context),
                storeMalformed,
                nullValue,
                multiFieldsBuilder.build(this, context),
                copyTo,
                preReleaseSubfield.build(context),
                majorVersionSubField.build(context),
                minorVersionSubField.build(context),
                patchVersionSubField.build(context)
            );
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        public TypeParser() {}

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
                    builder.nullValue(propNode.toString());
                    iterator.remove();
                } else if (propName.equals("store_malformed")) {
                    builder.storeMalformed(XContentMapValues.nodeBooleanValue(propNode, name + ".store_malformed"));
                    iterator.remove();
                } else if (TypeParsers.parseMultiField(builder::addMultiField, name, parserContext, propName, propNode)) {
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class VersionStringFieldType extends TermBasedFieldType {

        // if true, we want to throw errors on illegal versions at index and query time
        private boolean validateVersion = false;

        public VersionStringFieldType(
            String name,
            boolean isSearchable,
            boolean validateVersion,
            Map<String, String> meta,
            float boost,
            FieldType fieldType
        ) {
            super(name, isSearchable, true, new TextSearchInfo(fieldType, null, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER), meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setBoost(boost);
            this.validateVersion = validateVersion;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            if (context.allowExpensiveQueries() == false) {
                throw new ElasticsearchException(
                    "[prefix] queries cannot be executed when '"
                        + ALLOW_EXPENSIVE_QUERIES.getKey()
                        + "' is set to false. For optimised prefix queries on text "
                        + "fields please enable [index_prefixes]."
                );
            }
            failIfNotIndexed();
            return wildcardQuery(value + "*", method, context);
        }

        @Override
        public Query wildcardQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            if (context.allowExpensiveQueries() == false) {
                throw new ElasticsearchException(
                    "[wildcard] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
                );
            }
            failIfNotIndexed();

            VersionFieldWildcardQuery query = new VersionFieldWildcardQuery(new Term(name(), value));
            QueryParsers.setRewriteMethod(query, method);
            return query;
        }

        @Override
        protected BytesRef indexedValueForSearch(Object value) {
            String valueAsString;
            if (value instanceof String) {
                valueAsString = (String) value;
            } else if (value instanceof BytesRef) {
                // encoded string, need to re-encode
                valueAsString = ((BytesRef) value).utf8ToString();
            } else {
                throw new IllegalArgumentException("Illegal value type: " + value.getClass() + ", value: " + value);
            }
            EncodedVersion encodedVersion = encodeVersion(valueAsString);
            if (encodedVersion.isLegal == false && validateVersion) {
                throw new IllegalArgumentException("Illegal version string: " + valueAsString);
            }
            return encodedVersion.bytesRef;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(VersionScriptDocValues::new, CoreValuesSourceType.BYTES);
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return VERSION_DOCVALUE.format((BytesRef) value);
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
            }
            if (timeZone != null) {
                throw new IllegalArgumentException(
                    "Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones"
                );
            }
            return VERSION_DOCVALUE;
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            if (context.allowExpensiveQueries() == false) {
                throw new ElasticsearchException(
                    "[range] queries on [version] fields cannot be executed when '"
                        + ALLOW_EXPENSIVE_QUERIES.getKey()
                        + "' is set to false."
                );
            }
            failIfNotIndexed();
            BytesRef lower = lowerTerm == null ? null : indexedValueForSearch(lowerTerm);
            BytesRef upper = upperTerm == null ? null : indexedValueForSearch(upperTerm);
            byte[] lowerBytes = lower == null ? MIN_VALUE : Arrays.copyOfRange(lower.bytes, lower.offset, 16);
            byte[] upperBytes = upper == null ? MAX_VALUE : Arrays.copyOfRange(upper.bytes, upper.offset, 16);

            // point query on the 16byte prefix
            Query pointPrefixQuery = BinaryPoint.newRangeQuery(name(), lowerBytes, upperBytes);

            Query termQuery = new TermRangeQuery(
                name(),
                lowerTerm == null ? null : lower,
                upperTerm == null ? null : upper,
                includeLower,
                includeUpper
            );

            return new BooleanQuery.Builder().add(new BooleanClause(pointPrefixQuery, Occur.MUST))
                .add(new BooleanClause(termQuery, Occur.MUST))
                .build();
        }
    }

    private boolean storeMalformed;
    private String nullValue;
    private BooleanFieldMapper prereleaseSubField;
    private NumberFieldMapper majorVersionSubField;
    private NumberFieldMapper minorVersionSubField;
    private NumberFieldMapper patchVersionSubField;

    private VersionStringFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
        boolean storeMalformed,
        String nullValue,
        MultiFields multiFields,
        CopyTo copyTo,
        BooleanFieldMapper preReleaseMapper,
        NumberFieldMapper majorVersionMapper,
        NumberFieldMapper minorVersionMapper,
        NumberFieldMapper patchVersionMapper
    ) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        this.storeMalformed = storeMalformed;
        this.nullValue = nullValue;
        this.prereleaseSubField = preReleaseMapper;
        this.majorVersionSubField = majorVersionMapper;
        this.minorVersionSubField = minorVersionMapper;
        this.patchVersionSubField = patchVersionMapper;
    }

    @Override
    public VersionStringFieldType fieldType() {
        return (VersionStringFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected VersionStringFieldMapper clone() {
        return (VersionStringFieldMapper) super.clone();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        String versionString;
        if (context.externalValueSet()) {
            versionString = context.externalValue().toString();
        } else {
            XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                versionString = nullValue;
            } else {
                versionString = parser.textOrNull();
            }
        }

        if (versionString == null) {
            return;
        }

        EncodedVersion encoding = encodeVersion(versionString);
        if (encoding.isLegal == false && storeMalformed == false) {
            throw new IllegalArgumentException("Illegal version string: " + versionString);
        }
        BytesRef encodedVersion = encoding.bytesRef;
        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new Field(fieldType().name(), encodedVersion, fieldType);
            context.doc().add(field);
            // encode the first 16 bytes as points for efficient range query
            byte[] first16bytes = Arrays.copyOfRange(encodedVersion.bytes, encodedVersion.offset, 16);
            context.doc().add(new BinaryPoint(fieldType().name(), first16bytes));
        }
        context.doc().add(new SortedSetDocValuesField(fieldType().name(), encodedVersion));

        // add additional information extracted from version string
        context.doc().add(new Field(prereleaseSubField.name(), encoding.isPreRelease ? "T" : "F", BooleanFieldMapper.Defaults.FIELD_TYPE));
        context.doc().add(new SortedNumericDocValuesField(prereleaseSubField.name(), encoding.isPreRelease ? 1 : 0));

        addVersionPartSubfield(context, majorVersionSubField.name(), encoding.major);
        addVersionPartSubfield(context, minorVersionSubField.name(), encoding.minor);
        addVersionPartSubfield(context, patchVersionSubField.name(), encoding.patch);
    }

    private void addVersionPartSubfield(ParseContext context, String fieldName, Integer versionPart) {
        if (versionPart != null) {
            context.doc().addAll(NumberType.INTEGER.createFields(fieldName, versionPart, true, true, false));
        }
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        VersionStringFieldMapper mergeWith = (VersionStringFieldMapper) other;
        this.storeMalformed = mergeWith.storeMalformed;
        this.nullValue = mergeWith.nullValue;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
        builder.field("store_malformed", storeMalformed);
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> subIterators = new ArrayList<>();
        subIterators.add(prereleaseSubField);
        subIterators.add(majorVersionSubField);
        subIterators.add(minorVersionSubField);
        subIterators.add(patchVersionSubField);
        @SuppressWarnings("unchecked")
        Iterator<Mapper> concat = Iterators.concat(super.iterator(), subIterators.iterator());
        return concat;
    }

    private static DocValueFormat VERSION_DOCVALUE = new DocValueFormat() {

        @Override
        public String getWriteableName() {
            return "version_semver";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public String format(BytesRef value) {
            return VersionEncoder.decodeVersion(value);
        }

        @Override
        public BytesRef parseBytesRef(String value) {
            return VersionEncoder.encodeVersion(value).bytesRef;
        }

        @Override
        public String toString() {
            return getWriteableName();
        }
    };
}
