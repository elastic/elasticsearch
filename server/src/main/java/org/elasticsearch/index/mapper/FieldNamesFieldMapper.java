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

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * A mapper that indexes the field names of a document under <code>_field_names</code>. This mapper is typically useful in order
 * to have fast <code>exists</code> and <code>missing</code> queries/filters.
 *
 * Added in Elasticsearch 1.3.
 */
public class FieldNamesFieldMapper extends MetadataFieldMapper {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(FieldNamesFieldMapper.class));

    public static final String NAME = "_field_names";

    public static final String CONTENT_TYPE = "_field_names";

    public static class Defaults {
        public static final String NAME = FieldNamesFieldMapper.NAME;

        public static final boolean ENABLED = true;
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    static class Builder extends MetadataFieldMapper.Builder<Builder> {
        private boolean enabled = Defaults.ENABLED;

        Builder() {
            super(Defaults.NAME, Defaults.FIELD_TYPE);
        }

        Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        @Override
        public FieldNamesFieldMapper build(BuilderContext context) {
            FieldNamesFieldType fieldNamesFieldType = new FieldNamesFieldType();
            fieldNamesFieldType.setEnabled(enabled);
            return new FieldNamesFieldMapper(fieldType, fieldNamesFieldType);
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {

        public static final String ENABLED_DEPRECATION_MESSAGE = "Index [{}] uses the deprecated `enabled` setting for `_field_names`. "
                + "Disabling _field_names is not necessary because it no longer carries a large index overhead. Support for this setting "
                + "will be removed in a future major version. Please remove it from your mappings and templates.";

        @Override
        public MetadataFieldMapper.Builder<?> parse(String name, Map<String, Object> node,
                                                      ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder();

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    String indexName = parserContext.mapperService().index().getName();
                    if (parserContext.indexVersionCreated().onOrAfter(Version.V_8_0_0)) {
                        throw new MapperParsingException("The `enabled` setting for the `_field_names` field has been deprecated and "
                                + "removed but is still used in index [{}]. Please remove it from your mappings and templates.");
                    } else {
                        deprecationLogger.deprecate("field_names_enabled_parameter", ENABLED_DEPRECATION_MESSAGE, indexName);
                        builder.enabled(XContentMapValues.nodeBooleanValue(fieldNode, name + ".enabled"));
                    }
                    iterator.remove();
                }
            }
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(ParserContext context) {
            final Settings indexSettings = context.mapperService().getIndexSettings().getSettings();
            return new FieldNamesFieldMapper(Defaults.FIELD_TYPE, new FieldNamesFieldType());
        }
    }

    public static final class FieldNamesFieldType extends TermBasedFieldType {

        private boolean enabled = Defaults.ENABLED;

        public FieldNamesFieldType() {
            super(Defaults.NAME, true, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
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

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException("Cannot run exists query on _field_names");
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            if (isEnabled() == false) {
                throw new IllegalStateException("Cannot run [exists] queries if the [_field_names] field is disabled");
            }
            deprecationLogger.deprecate("terms_query_on_field_names",
                "terms query on the _field_names field is deprecated and will be removed, use exists query instead");
            return super.termQuery(value, context);
        }
    }

    private FieldNamesFieldMapper(FieldType fieldType, MappedFieldType mappedFieldType) {
        super(fieldType, mappedFieldType);
    }

    @Override
    public FieldNamesFieldType fieldType() {
        return (FieldNamesFieldType) super.fieldType();
    }

    @Override
    public void preParse(ParseContext context) {
    }

    @Override
    public void postParse(ParseContext context) {
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        // Adding values to the _field_names field is handled by the mappers for each field type
    }

    static Iterable<String> extractFieldNames(final String fullPath) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<>() {
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
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }

                };
            }
        };
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        if (includeDefaults == false && fieldType().isEnabled() == Defaults.ENABLED) {
            return builder;
        }

        builder.startObject(NAME);
        if (includeDefaults || fieldType().isEnabled() != Defaults.ENABLED) {
            builder.field("enabled", fieldType().isEnabled());
        }

        builder.endObject();
        return builder;
    }

}
