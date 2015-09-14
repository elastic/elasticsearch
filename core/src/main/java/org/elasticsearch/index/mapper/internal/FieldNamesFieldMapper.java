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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 * A mapper that indexes the field names of a document under <code>_field_names</code>. This mapper is typically useful in order
 * to have fast <code>exists</code> and <code>missing</code> queries/filters.
 *
 * Added in Elasticsearch 1.3.
 */
public class FieldNamesFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_field_names";

    public static final String CONTENT_TYPE = "_field_names";

    public static class Defaults {
        public static final String NAME = FieldNamesFieldMapper.NAME;
        
        public static final boolean ENABLED = true;
        public static final MappedFieldType FIELD_TYPE = new FieldNamesFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setNames(new MappedFieldType.Names(NAME));
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, FieldNamesFieldMapper> {
        private boolean enabled = Defaults.ENABLED;

        public Builder(MappedFieldType existing) {
            super(Defaults.NAME, existing == null ? Defaults.FIELD_TYPE : existing);
            indexName = Defaults.NAME;
        }

        @Override
        @Deprecated
        public Builder index(boolean index) {
            enabled(index);
            return super.index(index);
        }
        
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        @Override
        public FieldNamesFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            fieldType.setHasDocValues(false);
            FieldNamesFieldType fieldNamesFieldType = (FieldNamesFieldType)fieldType;
            fieldNamesFieldType.setEnabled(enabled);
            return new FieldNamesFieldMapper(fieldType, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (parserContext.indexVersionCreated().before(Version.V_1_3_0)) {
                throw new IllegalArgumentException("type="+CONTENT_TYPE+" is not supported on indices created before version 1.3.0. Is your cluster running multiple datanode versions?");
            }
            
            Builder builder = new Builder(parserContext.mapperService().fullName(NAME));
            if (parserContext.indexVersionCreated().before(Version.V_2_0_0_beta1)) {
                parseField(builder, builder.name, node, parserContext);
            }

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(nodeBooleanValue(fieldNode));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class FieldNamesFieldType extends MappedFieldType {

        private boolean enabled = Defaults.ENABLED;

        public FieldNamesFieldType() {
            setFieldDataType(new FieldDataType("string"));
        }

        protected FieldNamesFieldType(FieldNamesFieldType ref) {
            super(ref);
            this.enabled = ref.enabled;
        }

        @Override
        public FieldNamesFieldType clone() {
            return new FieldNamesFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            FieldNamesFieldType that = (FieldNamesFieldType) o;
            return enabled == that.enabled;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), enabled);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public void checkCompatibility(MappedFieldType fieldType, List<String> conflicts, boolean strict) {
            super.checkCompatibility(fieldType, conflicts, strict);
            if (strict) {
                FieldNamesFieldType other = (FieldNamesFieldType)fieldType;
                if (isEnabled() != other.isEnabled()) {
                    conflicts.add("mapper [" + names().fullName() + "] is used by multiple types. Set update_all_types to true to update [enabled] across all types.");
                }
            }
        }

        public void setEnabled(boolean enabled) {
            checkIfFrozen();
            this.enabled = enabled;
        }

        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public String value(Object value) {
            if (value == null) {
                return null;
            }
            return value.toString();
        }

        @Override
        public boolean useTermQueryWithQueryString() {
            return true;
        }
    }

    private final boolean pre13Index; // if the index was created before 1.3, _field_names is always disabled

    public FieldNamesFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(existing == null ? Defaults.FIELD_TYPE.clone() : existing.clone(), indexSettings);
    }

    public FieldNamesFieldMapper(MappedFieldType fieldType, Settings indexSettings) {
        super(NAME, fieldType, Defaults.FIELD_TYPE, indexSettings);
        this.pre13Index = Version.indexCreated(indexSettings).before(Version.V_1_3_0);
        if (this.pre13Index) {
            FieldNamesFieldType newFieldType = fieldType().clone();
            newFieldType.setEnabled(false);
            newFieldType.freeze();
            fieldTypeRef.set(newFieldType);
        }
    }

    @Override
    public FieldNamesFieldType fieldType() {
        return (FieldNamesFieldType) super.fieldType();
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // we parse in post parse
        return null;
    }

    static Iterable<String> extractFieldNames(final String fullPath) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {

                    int endIndex = nextEndIndex(0);

                    private int nextEndIndex(int index) {
                        while (index < fullPath.length() && fullPath.charAt(index) != '.') {
                            index += 1;
                        }
                        return index;
                    }

                    @Override
                    public boolean hasNext() {
                        return endIndex <= fullPath.length();
                    }

                    @Override
                    public String next() {
                        final String result = fullPath.substring(0, endIndex);
                        endIndex = nextEndIndex(endIndex + 1);
                        return result;
                    }

                    @Override
                    public final void remove() {
                        throw new UnsupportedOperationException();
                    }

                };
            }
        };
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (fieldType().isEnabled() == false) {
            return;
        }
        for (ParseContext.Document document : context.docs()) {
            final List<String> paths = new ArrayList<>();
            for (IndexableField field : document.getFields()) {
                paths.add(field.name());
            }
            for (String path : paths) {
                for (String fieldName : extractFieldNames(path)) {
                    if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
                        document.add(new Field(fieldType().names().indexName(), fieldName, fieldType()));
                    }
                }
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (pre13Index) {
            return builder;
        }
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        if (includeDefaults == false && fieldType().isEnabled() == Defaults.ENABLED) {
            return builder;
        }
        
        builder.startObject(NAME);
        if (includeDefaults || fieldType().isEnabled() != Defaults.ENABLED) {
            builder.field("enabled", fieldType().isEnabled());
        }
        if (indexCreatedBefore2x && (includeDefaults || fieldType().equals(Defaults.FIELD_TYPE) == false)) {
            super.doXContentBody(builder, includeDefaults, params);
        }
        
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isGenerated() {
        return true;
    }
}
