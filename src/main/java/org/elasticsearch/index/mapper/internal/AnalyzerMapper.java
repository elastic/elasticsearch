/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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

        public Builder() {
            super(CONTENT_TYPE);
            this.builder = this;
        }

        public Builder field(String field) {
            this.field = field;
            return this;
        }

        @Override
        public AnalyzerMapper build(BuilderContext context) {
            return new AnalyzerMapper(field);
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
                }
            }
            return builder;
        }
    }

    private final String path;

    public AnalyzerMapper() {
        this(Defaults.PATH);
    }

    public AnalyzerMapper(String path) {
        this.path = path.intern();
    }

    @Override
    public String name() {
        return CONTENT_TYPE;
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
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (path.equals(Defaults.PATH)) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (!path.equals(Defaults.PATH)) {
            builder.field("path", path);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void close() {

    }
}
