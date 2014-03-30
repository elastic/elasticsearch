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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeMapValue;
import static org.elasticsearch.index.mapper.MapperBuilders.analyzer;

/**
 *
 */
public class AnalyzerMapper implements Mapper, InternalMapper, RootMapper {

    public static final String NAME = "_analyzer";
    public static final String CONTENT_TYPE = "_analyzer";

    public static class Defaults {
        public static final String PATH = "_analyzer";
    }

    public static class Builder extends Mapper.Builder<Builder, AnalyzerMapper> {

        private String field = Defaults.PATH;

        private ImmutableMap<String, Object> meta;

        public Builder() {
            super(CONTENT_TYPE);
            this.builder = this;
        }

        public Builder field(String field) {
            this.field = field;
            return this;
        }

        public Builder meta(ImmutableMap<String, Object> meta) {
            this.meta = meta;
            return this;
        }

        @Override
        public AnalyzerMapper build(BuilderContext context) {
            return new AnalyzerMapper(field, meta);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            AnalyzerMapper.Builder builder = analyzer();
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    builder.field(fieldNode.toString());
                } else if (fieldName.equals("_meta")) {
                    builder.meta(ImmutableMap.copyOf(nodeMapValue(fieldNode, "_meta")));
                }
            }
            return builder;
        }
    }

    private final String path;

    private ImmutableMap<String, Object> meta;

    public AnalyzerMapper() {
        this(Defaults.PATH, null);
    }

    public AnalyzerMapper(String path, ImmutableMap<String, Object> meta) {
        this.path = path.intern();
        this.meta = meta;
    }

    @Override
    public String name() {
        return CONTENT_TYPE;
    }

    public ImmutableMap<String, Object> meta() {
        return meta;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        Analyzer analyzer = context.docMapper().mappers().indexAnalyzer();
        if (path != null) {
            String value = null;
            List<IndexableField> fields = context.doc().getFields();
            for (int i = 0, fieldsSize = fields.size(); i < fieldsSize; i++) {
                IndexableField field = fields.get(i);
                if (field.name() == path) {
                    value = field.stringValue();
                    break;
                }
            }
            if (value == null) {
                value = context.ignoredValue(path);
            }
            if (value != null) {
                analyzer = context.analysisService().analyzer(value);
                if (analyzer == null) {
                    throw new MapperParsingException("No analyzer found for [" + value + "] from path [" + path + "]");
                }
                analyzer = context.docMapper().mappers().indexAnalyzer(analyzer);
            }
        }
        context.analyzer(analyzer);
    }

    @Override
    public void validate(ParseContext context) throws MapperParsingException {
    }

    @Override
    public boolean includeInObject() {
        return false;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        AnalyzerMapper fieldMergeWith = (AnalyzerMapper) mergeWith;
        if (!mergeContext.mergeFlags().simulate()) {
            this.meta = fieldMergeWith.meta;
        }
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (path.equals(Defaults.PATH) && (meta == null || meta.isEmpty())) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (!path.equals(Defaults.PATH)) {
            builder.field("path", path);
        }
        if (meta != null && !meta.isEmpty()) {
            builder.field("_meta", meta);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void close() {

    }
}
