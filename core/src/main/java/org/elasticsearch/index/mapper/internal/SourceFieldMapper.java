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
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.lenientNodeBooleanValue;

/**
 *
 */
public class SourceFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_source";

    public static final String CONTENT_TYPE = "_source";

    public static class Defaults {
        public static final String NAME = SourceFieldMapper.NAME;
        public static final boolean ENABLED = true;

        public static final MappedFieldType FIELD_TYPE = new SourceFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE); // not indexed
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.freeze();
        }

    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, SourceFieldMapper> {

        private boolean enabled = Defaults.ENABLED;

        private String[] includes = null;
        private String[] excludes = null;

        public Builder() {
            super(Defaults.NAME, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder includes(String[] includes) {
            this.includes = includes;
            return this;
        }

        public Builder excludes(String[] excludes) {
            this.excludes = excludes;
            return this;
        }

        @Override
        public SourceFieldMapper build(BuilderContext context) {
            return new SourceFieldMapper(enabled, includes, excludes, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder();

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(lenientNodeBooleanValue(fieldNode));
                    iterator.remove();
                } else if ("format".equals(fieldName) && parserContext.indexVersionCreated().before(Version.V_5_0_0_alpha1)) {
                    // ignore on old indices, reject on and after 5.0
                    iterator.remove();
                } else if (fieldName.equals("includes")) {
                    List<Object> values = (List<Object>) fieldNode;
                    String[] includes = new String[values.size()];
                    for (int i = 0; i < includes.length; i++) {
                        includes[i] = values.get(i).toString();
                    }
                    builder.includes(includes);
                    iterator.remove();
                } else if (fieldName.equals("excludes")) {
                    List<Object> values = (List<Object>) fieldNode;
                    String[] excludes = new String[values.size()];
                    for (int i = 0; i < excludes.length; i++) {
                        excludes[i] = values.get(i).toString();
                    }
                    builder.excludes(excludes);
                    iterator.remove();
                }
            }
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(Settings indexSettings, MappedFieldType fieldType, String typeName) {
            return new SourceFieldMapper(indexSettings);
        }
    }

    static final class SourceFieldType extends MappedFieldType {

        public SourceFieldType() {}

        protected SourceFieldType(SourceFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new SourceFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "The _source field is not searchable");
        }
    }

    private final boolean enabled;

    /** indicates whether the source will always exist and be complete, for use by features like the update API */
    private final boolean complete;

    private final String[] includes;
    private final String[] excludes;

    private SourceFieldMapper(Settings indexSettings) {
        this(Defaults.ENABLED, null, null, indexSettings);
    }

    private SourceFieldMapper(boolean enabled, String[] includes, String[] excludes, Settings indexSettings) {
        super(NAME, Defaults.FIELD_TYPE.clone(), Defaults.FIELD_TYPE, indexSettings); // Only stored.
        this.enabled = enabled;
        this.includes = includes;
        this.excludes = excludes;
        this.complete = enabled && includes == null && excludes == null;
    }

    public boolean enabled() {
        return enabled;
    }

    public String[] excludes() {
        return this.excludes != null ? this.excludes : Strings.EMPTY_ARRAY;

    }

    public String[] includes() {
        return this.includes != null ? this.includes : Strings.EMPTY_ARRAY;
    }

    public boolean isComplete() {
        return complete;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // nothing to do here, we will call it in pre parse
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (!enabled) {
            return;
        }
        if (!fieldType().stored()) {
            return;
        }
        BytesReference source = context.sourceToParse().source();
        // Percolate and tv APIs may not set the source and that is ok, because these APIs will not index any data
        if (source == null) {
            return;
        }

        boolean filtered = (includes != null && includes.length > 0) || (excludes != null && excludes.length > 0);
        if (filtered) {
            // we don't update the context source if we filter, we want to keep it as is...

            Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(source, true);
            Map<String, Object> filteredSource = XContentMapValues.filter(mapTuple.v2(), includes, excludes);
            BytesStreamOutput bStream = new BytesStreamOutput();
            XContentType contentType = mapTuple.v1();
            XContentBuilder builder = XContentFactory.contentBuilder(contentType, bStream).map(filteredSource);
            builder.close();

            source = bStream.bytes();
        }
        if (!source.hasArray()) {
            source = source.toBytesArray();
        }
        fields.add(new StoredField(fieldType().name(), source.array(), source.arrayOffset(), source.length()));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        // all are defaults, no need to write it at all
        if (!includeDefaults && enabled == Defaults.ENABLED && includes == null && excludes == null) {
            return builder;
        }
        builder.startObject(contentType());
        if (includeDefaults || enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }

        if (includes != null) {
            builder.field("includes", includes);
        } else if (includeDefaults) {
            builder.field("includes", Strings.EMPTY_ARRAY);
        }

        if (excludes != null) {
            builder.field("excludes", excludes);
        } else if (includeDefaults) {
            builder.field("excludes", Strings.EMPTY_ARRAY);
        }

        builder.endObject();
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        SourceFieldMapper sourceMergeWith = (SourceFieldMapper) mergeWith;
        List<String> conflicts = new ArrayList<>();
        if (this.enabled != sourceMergeWith.enabled) {
            conflicts.add("Cannot update enabled setting for [_source]");
        }
        if (Arrays.equals(includes(), sourceMergeWith.includes()) == false) {
            conflicts.add("Cannot update includes setting for [_source]");
        }
        if (Arrays.equals(excludes(), sourceMergeWith.excludes()) == false) {
            conflicts.add("Cannot update excludes setting for [_source]");
        }
        if (conflicts.isEmpty() == false) {
            throw new IllegalArgumentException("Can't merge because of conflicts: " + conflicts);
        }
    }
}
