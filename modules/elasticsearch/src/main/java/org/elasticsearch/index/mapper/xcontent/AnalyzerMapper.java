/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.mapper.xcontent;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class AnalyzerMapper implements XContentMapper {

    public static final String CONTENT_TYPE = "_analyzer";

    public static class Defaults {
        public static final String PATH = "_analyzer";
    }

    public static class Builder extends XContentMapper.Builder<Builder, AnalyzerMapper> {

        private String field = Defaults.PATH;

        public Builder() {
            super(CONTENT_TYPE);
            this.builder = this;
        }

        public Builder field(String field) {
            this.field = field;
            return this;
        }

        @Override public AnalyzerMapper build(BuilderContext context) {
            return new AnalyzerMapper(field);
        }
    }

    // for now, it is parsed directly in the document parser, need to move this internal types parsing to be done here as well...
//    public static class TypeParser implements XContentMapper.TypeParser {
//        @Override public XContentMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
//            AnalyzerMapper.Builder builder = analyzer();
//            for (Map.Entry<String, Object> entry : node.entrySet()) {
//                String fieldName = Strings.toUnderscoreCase(entry.getKey());
//                Object fieldNode = entry.getValue();
//                if ("path".equals(fieldName)) {
//                    builder.field(fieldNode.toString());
//                }
//            }
//            return builder;
//        }
//    }

    private final String path;

    public AnalyzerMapper() {
        this(Defaults.PATH);
    }

    public AnalyzerMapper(String path) {
        this.path = path;
    }

    @Override public String name() {
        return CONTENT_TYPE;
    }

    @Override public void parse(ParseContext context) throws IOException {
        Analyzer analyzer = context.docMapper().mappers().indexAnalyzer();
        if (path != null) {
            String value = context.doc().get(path);
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

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
    }

    @Override public void traverse(FieldMapperListener fieldMapperListener) {
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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

    @Override public void close() {

    }
}
