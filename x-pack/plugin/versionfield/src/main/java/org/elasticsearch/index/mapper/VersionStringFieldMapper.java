/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordField;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xpack.versionfield.VersionEncoder;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.elasticsearch.xpack.versionfield.VersionEncoder.encodeVersion;

/** A {@link FieldMapper} for software versions. */
public class VersionStringFieldMapper extends FieldMapper {

    // TODO naming etc... wrt VersionFieldMapper
    public static final String CONTENT_TYPE = "version";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }

        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final String NULL_VALUE = null;
        public static final int IGNORE_ABOVE = Integer.MAX_VALUE;
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        private Explicit<Boolean> ignoreMalformed = new Explicit<Boolean>(false, false);
        protected String nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = new Explicit<Boolean>(ignoreMalformed, true);
            return builder;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return ignoreMalformed;
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.IGNORE_MALFORMED;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return builder;
        }

        private VersionStringFieldType buildFieldType(BuilderContext context) {
            return new VersionStringFieldType(buildFullName(context), indexed, hasDocValues, meta, boost, fieldType);
        }

        @Override
        public VersionStringFieldMapper build(BuilderContext context) {
            BooleanFieldMapper.Builder preReleaseSubfield = new BooleanFieldMapper.Builder(name + ".isPreRelease");
            NumberFieldMapper.Builder majorVersionSubField = new NumberFieldMapper.Builder(name + ".major", NumberType.INTEGER);
            NumberFieldMapper.Builder minorVersionSubField = new NumberFieldMapper.Builder(name + ".minor", NumberType.INTEGER);

            return new VersionStringFieldMapper(
                name,
                fieldType,
                buildFieldType(context),
                ignoreMalformed,
                nullValue,
                multiFieldsBuilder.build(this, context),
                copyTo,
                preReleaseSubfield.build(context),
                majorVersionSubField.build(context),
                minorVersionSubField.build(context)
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
                } else if (propName.equals("ignore_malformed")) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode, name + ".ignore_malformed"));
                    iterator.remove();
                } else if (TypeParsers.parseMultiField(builder, name, parserContext, propName, propNode)) {
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class VersionStringFieldType extends TermBasedFieldType {

        public VersionStringFieldType(
            String name,
            boolean isSearchable,
            boolean hasDocValues,
            Map<String, String> meta,
            float boost,
            FieldType fieldType
        ) {
            super(
                name,
                isSearchable,
                hasDocValues,
                new TextSearchInfo(fieldType, null, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER),
                meta
            );
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setBoost(boost);
        }

        VersionStringFieldType(VersionStringFieldType other) {
            super(other);
        }

        @Override
        public MappedFieldType clone() {
            return new VersionStringFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
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
            if (value instanceof String) {
                return encodeVersion((String) value);
            } else if (value instanceof BytesRef) {
                // encoded string, need to re-encode
                return encodeVersion(((BytesRef) value).utf8ToString());
            } else {
                throw new IllegalArgumentException("Illegal value type: " + value.getClass() + ", value: " + value);
            }
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            failIfNotIndexed();
            Query query;
            try {
                query = new TermQuery(new Term(name(), indexedValueForSearch(value)));
            } catch (IllegalArgumentException e) {
                query = new TermQuery(new Term(name() + ".original", BytesRefs.toBytesRef(value)));
            }
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            failIfNotIndexed();
            BytesRef[] bytesRefs = new BytesRef[values.size()];
            for (int i = 0; i < bytesRefs.length; i++) {
                bytesRefs[i] = indexedValueForSearch(values.get(i));
            }
            return new TermInSetQuery(name(), bytesRefs);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            // TODO adrien "we'll need to extend it to return proper version strings in scripts".
            return new SortedSetOrdinalsIndexFieldData.Builder(CoreValuesSourceType.BYTES);
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return VersionEncoder.VERSION_DOCVALUE.format((BytesRef) value);
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
            return VersionEncoder.VERSION_DOCVALUE;
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
            // TODO adrien: run the range on points and doc values
            return new TermRangeQuery(
                name(),
                lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                upperTerm == null ? null : indexedValueForSearch(upperTerm),
                includeLower,
                includeUpper
            );
        }
    }

    private Explicit<Boolean> ignoreMalformed;
    private String nullValue;
    private BooleanFieldMapper prereleaseSubField;
    private NumberFieldMapper majorVersionSubField;
    private NumberFieldMapper minorVersionSubField;

    VersionStringFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
        Explicit<Boolean> ignoreMalformed,
        String nullValue,
        MultiFields multiFields,
        CopyTo copyTo,
        BooleanFieldMapper preReleaseMapper,
        NumberFieldMapper majorVersionMapper,
        NumberFieldMapper minorVersionMapper
    ) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.nullValue = nullValue;
        this.prereleaseSubField = preReleaseMapper;
        this.majorVersionSubField = majorVersionMapper;
        this.minorVersionSubField = minorVersionMapper;
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

        BytesRef encodedVersion = null;
        try {
            encodedVersion = encodeVersion(versionString);
        } catch (IllegalArgumentException e) {
            if (ignoreMalformed.value()) {
                context.addIgnoredField(name());
                // enable exact term searches on hidden field in this case
                context.doc()
                    .add(
                        new KeywordField(
                            fieldType().name() + ".original",
                            new BytesRef(versionString),
                            KeywordFieldMapper.Defaults.FIELD_TYPE
                        )
                    );
                return;
            } else {
                throw e;
            }
        }
        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            // TODO adrien: encode the first 16 bytes as points for efficient range query
            Field field = new Field(fieldType().name(), encodedVersion, fieldType);
            context.doc().add(field);
            byte[] first16bytes = Arrays.copyOfRange(encodedVersion.bytes, encodedVersion.offset, 16);
            context.doc().add(new BinaryPoint(fieldType().name(), first16bytes));

            if (fieldType().hasDocValues() == false && fieldType.omitNorms()) {
                createFieldNamesField(context);
            }
        }
        // add prerelease flag
        boolean isPrerelease = versionString.contains("-");
        context.doc().add(new Field(prereleaseSubField.name(), isPrerelease ? "T" : "F", BooleanFieldMapper.Defaults.FIELD_TYPE));
        context.doc().add(new SortedNumericDocValuesField(prereleaseSubField.name(), isPrerelease ? 1 : 0));

        boolean docValued = fieldType().hasDocValues();
        boolean stored = fieldType.stored();
        // add major version
        Integer major = Integer.valueOf(versionString.substring(0, versionString.indexOf('.')));
        context.doc()
            .addAll(NumberType.INTEGER.createFields(majorVersionSubField.name(), major, fieldType().isSearchable(), docValued, stored));

        // add minor version
        Integer minor = Integer.valueOf(
            versionString.substring(versionString.indexOf('.') + 1, versionString.indexOf('.', versionString.indexOf('.') + 1))
        );
        context.doc()
            .addAll(NumberType.INTEGER.createFields(minorVersionSubField.name(), minor, fieldType().isSearchable(), docValued, stored));

        if (fieldType().hasDocValues()) {
            context.doc().add(new SortedSetDocValuesField(fieldType().name(), encodedVersion));
        }
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        VersionStringFieldMapper mergeWith = (VersionStringFieldMapper) other;
        if (mergeWith.ignoreMalformed.explicit()) {
            this.ignoreMalformed = mergeWith.ignoreMalformed;
        }
        this.prereleaseSubField = (BooleanFieldMapper) this.prereleaseSubField.merge(mergeWith.prereleaseSubField);
        this.nullValue = mergeWith.nullValue;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }

        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field("ignore_malformed", ignoreMalformed.value());
        }
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> subIterators = new ArrayList<>();
        subIterators.add(prereleaseSubField);
        subIterators.add(majorVersionSubField);
        subIterators.add(minorVersionSubField);
        @SuppressWarnings("unchecked")
        Iterator<Mapper> concat = Iterators.concat(super.iterator(), subIterators.iterator());
        return concat;
    }
}
