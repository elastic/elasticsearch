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
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/**
 * A field mapper for keywords. This mapper accepts strings and indexes them as-is.
 */
public final class KeywordFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "keyword";

    private static final List<String> SUPPORTED_PARAMETERS_FOR_AUTO_DOWNGRADE_TO_STRING = unmodifiableList(Arrays.asList(
            "type",
            // common keyword parameters, for which the upgrade is straightforward
            "index", "store", "doc_values", "omit_norms", "norms", "boost", "fields", "copy_to",
            "include_in_all", "ignore_above", "index_options", "similarity"));

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new KeywordFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }

        public static final String NULL_VALUE = null;
        public static final int IGNORE_ABOVE = Integer.MAX_VALUE;
    }

    public static class Builder extends FieldMapper.Builder<Builder, KeywordFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;
        protected int ignoreAbove = Defaults.IGNORE_ABOVE;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder ignoreAbove(int ignoreAbove) {
            if (ignoreAbove < 0) {
                throw new IllegalArgumentException("[ignore_above] must be positive, got " + ignoreAbove);
            }
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        @Override
        public Builder indexOptions(IndexOptions indexOptions) {
            if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) > 0) {
                throw new IllegalArgumentException("The [keyword] field does not support positions, got [index_options]="
                        + indexOptionToString(indexOptions));
            }
            return super.indexOptions(indexOptions);
        }

        public Builder eagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
            fieldType().setEagerGlobalOrdinals(eagerGlobalOrdinals);
            return builder;
        }

        @Override
        public KeywordFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new KeywordFieldMapper(
                    name, fieldType, defaultFieldType, ignoreAbove, includeInAll,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (parserContext.indexVersionCreated().before(Version.V_5_0_0_alpha1)) {
                // Downgrade "keyword" to "string" in indexes created in 2.x so you can use modern syntax against old indexes
                Set<String> unsupportedParameters = new HashSet<>(node.keySet());
                unsupportedParameters.removeAll(SUPPORTED_PARAMETERS_FOR_AUTO_DOWNGRADE_TO_STRING);
                if (false == SUPPORTED_PARAMETERS_FOR_AUTO_DOWNGRADE_TO_STRING.containsAll(node.keySet())) {
                    throw new IllegalArgumentException("Automatic downgrade from [keyword] to [string] failed because parameters "
                            + unsupportedParameters + " are not supported for automatic downgrades.");
                }
                {   // Downgrade "index"
                    Object index = node.get("index");
                    if (index == null || Boolean.TRUE.equals(index)) {
                        index = "not_analyzed";
                    } else if (Boolean.FALSE.equals(index)) {
                        index = "no";
                    } else {
                        throw new IllegalArgumentException(
                                "Can't parse [index] value [" + index + "] for field [" + name + "], expected [true] or [false]");
                    }
                    node.put("index", index);
                }
                
                return new StringFieldMapper.TypeParser().parse(name, node, parserContext);
            }
            KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
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
                } else if (propName.equals("ignore_above")) {
                    builder.ignoreAbove(XContentMapValues.nodeIntegerValue(propNode, -1));
                    iterator.remove();
                } else if (propName.equals("norms")) {
                    builder.omitNorms(XContentMapValues.nodeBooleanValue(propNode) == false);
                    iterator.remove();
                } else if (propName.equals("eager_global_ordinals")) {
                    builder.eagerGlobalOrdinals(XContentMapValues.nodeBooleanValue(propNode));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class KeywordFieldType extends StringFieldType {

        public KeywordFieldType() {}

        protected KeywordFieldType(KeywordFieldType ref) {
            super(ref);
        }

        public KeywordFieldType clone() {
            return new KeywordFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query nullValueQuery() {
            if (nullValue() == null) {
                return null;
            }
            return termQuery(nullValue(), null);
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
            // keywords are internally stored as utf8 bytes
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }
    }

    private Boolean includeInAll;
    private int ignoreAbove;

    protected KeywordFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                int ignoreAbove, Boolean includeInAll, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        this.ignoreAbove = ignoreAbove;
        this.includeInAll = includeInAll;
    }

    /** Values that have more chars than the return value of this method will
     *  be skipped at parsing time. */
    // pkg-private for testing
    int ignoreAbove() {
        return ignoreAbove;
    }

    @Override
    protected KeywordFieldMapper clone() {
        return (KeywordFieldMapper) super.clone();
    }

    // pkg-private for testing
    Boolean includeInAll() {
        return includeInAll;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        final String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                value = fieldType().nullValueAsString();
            } else {
                value =  parser.textOrNull();
            }
        }

        if (value == null || value.length() > ignoreAbove) {
            return;
        }

        if (context.includeInAll(includeInAll, this)) {
            context.allEntries().addText(fieldType().name(), value, fieldType().boost());
        }

        // convert to utf8 only once before feeding postings/dv/stored fields
        final BytesRef binaryValue = new BytesRef(value);
        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            Field field = new Field(fieldType().name(), binaryValue, fieldType());
            fields.add(field);
        }
        if (fieldType().hasDocValues()) {
            fields.add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        this.includeInAll = ((KeywordFieldMapper) mergeWith).includeInAll;
        this.ignoreAbove = ((KeywordFieldMapper) mergeWith).ignoreAbove;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field("null_value", fieldType().nullValue());
        }

        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", true);
        }

        if (includeDefaults || ignoreAbove != Defaults.IGNORE_ABOVE) {
            builder.field("ignore_above", ignoreAbove);
        }
    }
}
