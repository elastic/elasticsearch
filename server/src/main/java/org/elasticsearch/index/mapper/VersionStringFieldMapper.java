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


import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.VersionEncoder.SortMode;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.VersionEncoder.encodeVersion;
import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

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
        protected int ignoreAbove = Defaults.IGNORE_ABOVE;
        private SortMode mode = SortMode.SEMVER;

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

        public Builder ignoreAbove(int ignoreAbove) {
            if (ignoreAbove < 0) {
                throw new IllegalArgumentException("[ignore_above] must be positive, got " + ignoreAbove);
            }
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        private VersionStringFieldType buildFieldType(BuilderContext context) {
            return new VersionStringFieldType(buildFullName(context), indexed, hasDocValues, meta, boost, mode);
        }

        public void mode(SortMode mode) {
            this.mode = mode;
        }

        @Override
        public VersionStringFieldMapper build(BuilderContext context) {
            return new VersionStringFieldMapper(
                name,
                fieldType,
                buildFieldType(context),
                ignoreMalformed,
                ignoreAbove,
                nullValue,
                context.indexSettings(),
                multiFieldsBuilder.build(this, context),
                copyTo,
                mode
            );
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
                    builder.nullValue(propNode.toString());
                    iterator.remove();
                } else if (propName.equals("ignore_malformed")) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode, name + ".ignore_malformed"));
                    iterator.remove();
                } else if (propName.equals("mode")) {
                    builder.mode(SortMode.fromString(propNode.toString()));
                    iterator.remove();
                } else if (TypeParsers.parseMultiField(builder, name, parserContext, propName, propNode)) {
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class VersionStringFieldType extends TermBasedFieldType {

        private final SortMode mode;

        public VersionStringFieldType(
            String name,
            boolean isSearchable,
            boolean hasDocValues,
            Map<String, String> meta,
            float boost,
            SortMode mode
        ) {
            super(name, isSearchable, hasDocValues, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            setBoost(boost);
            this.mode = mode;
        }

        VersionStringFieldType(VersionStringFieldType other) {
            super(other);
            this.mode = other.mode;
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
                throw new ElasticsearchException("[prefix] queries cannot be executed when '" +
                        ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false. For optimised prefix queries on text " +
                        "fields please enable [index_prefixes].");
            }
            failIfNotIndexed();
            BytesRef encoded = indexedValueForSearch(value);
            if (encoded.bytes[encoded.length - 1] == VersionEncoder.NO_PRERELESE_SEPARATOR_BYTE) {
                encoded.length = encoded.length - 1;
            }
            PrefixQuery query = new PrefixQuery(new Term(name(), encoded));
            if (method != null) {
                query.setRewriteMethod(method);
            }
            return query;
        }

        @Override
        protected BytesRef indexedValueForSearch(Object value) {
            if (value instanceof String) {
                return encodeVersion((String) value, mode);
            } else if (value instanceof BytesRef) {
                // encoded string, need to re-encode
                return encodeVersion(((BytesRef) value).utf8ToString(), mode);
            } else {
                throw new IllegalArgumentException("Illegal value type: " + value.getClass());
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(CoreValuesSourceType.BYTES);
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            // keywords are internally stored as utf8 bytes
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
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
            return mode.docValueFormat();
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            if (context.allowExpensiveQueries() == false) {
                throw new ElasticsearchException("[range] queries on [version] fields cannot be executed when '" +
                        ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false.");
            }
            failIfNotIndexed();
            return new TermRangeQuery(name(),
                lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                upperTerm == null ? null : indexedValueForSearch(upperTerm),
                includeLower, includeUpper);
        }
    }

    private Explicit<Boolean> ignoreMalformed;
    private int ignoreAbove;
    private String nullValue;
    private SortMode mode;

    VersionStringFieldMapper(
            String simpleName,
            FieldType fieldType,
            MappedFieldType mappedFieldType,
            Explicit<Boolean> ignoreMalformed,
            int ignoreAbove,
            String nullValue,
            Settings indexSettings,
            MultiFields multiFields,
            CopyTo copyTo,
            SortMode mode) {
        super(simpleName, fieldType, mappedFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.ignoreAbove = ignoreAbove;
        this.nullValue = nullValue;
        this.mode = mode;
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
                versionString =  parser.textOrNull();
            }
        }

        if (versionString == null) {
            return;
        }

        BytesRef encodedVersion = null;
        try {
            encodedVersion = encodeVersion(versionString, mode);
        } catch (IllegalArgumentException e) {
            if (ignoreMalformed.value()) {
                context.addIgnoredField(name());
                return;
            } else {
                throw e;
            }
        }
        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored())  {
            Field field = new Field(fieldType().name(), encodedVersion, fieldType);
            context.doc().add(field);

            if (fieldType().hasDocValues() == false && fieldType.omitNorms()) {
                createFieldNamesField(context);
            }
        }

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
        this.ignoreAbove = mergeWith.ignoreAbove;
        this.nullValue = mergeWith.nullValue;
        this.mode = mergeWith.mode;
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
}
