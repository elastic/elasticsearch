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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.search.highlight.HighlighterContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.analyzer;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
public class AnalyzerMapper extends AbstractFieldMapper<String> implements InternalMapper, RootMapper {

    public static final String NAME = "_analyzer";
    public static final String CONTENT_TYPE = "_analyzer";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String PATH = "_analyzer";
        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);
    }

    @Override
    public String value(Object value) {
        return (String) value;
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, AnalyzerMapper> {

        private String field = Defaults.PATH;

        public Builder() {
            super(CONTENT_TYPE, new FieldType(Defaults.FIELD_TYPE));
            this.builder = this;
        }

        public Builder field(String field) {
            this.field = field;
            return this;
        }

        @Override
        public AnalyzerMapper build(BuilderContext context) {
            return new AnalyzerMapper(field, fieldType);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            AnalyzerMapper.Builder builder = analyzer();
            parseField(builder, name, node, parserContext);
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
        this(Defaults.PATH, Defaults.FIELD_TYPE);
    }

    protected AnalyzerMapper(String path, FieldType fieldType) {
        this(path, Defaults.BOOST, fieldType, null, null, null, null);
    }

    public AnalyzerMapper(String path, float boost, FieldType fieldType, PostingsFormatProvider postingsProvider,
                                 DocValuesFormatProvider docValuesProvider, @Nullable Settings fieldDataSettings, Settings indexSettings) {
        super(new Names(path, path, NAME, NAME), boost, fieldType, null, Lucene.KEYWORD_ANALYZER,
                Lucene.KEYWORD_ANALYZER, postingsProvider, docValuesProvider, null, null, fieldDataSettings, indexSettings);
        this.path = path.intern();
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("string");
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        if (context.analyzer() == null) {
            Analyzer analyzer = context.docMapper().mappers().indexAnalyzer();
            context.analyzer(analyzer);
        }
    }

    @Override
    public boolean includeInObject() {
        return true;
    }

    public Analyzer setAnalyzer(HighlighterContext context){
        if (context.analyzer() != null){
            return context.analyzer();
        }

        Analyzer analyzer = null;

        if (path != null) {
            String analyzerName = (String) context.context.lookup().source().extractValue(path);
            analyzer = context.context.mapperService().analysisService().analyzer(analyzerName);
        }

        if (analyzer == null) {
            analyzer = context.context.mapperService().documentMapper(context.hitContext.hit().type()).mappers().indexAnalyzer();
        }
        context.analyzer(analyzer);

        return analyzer;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        String value = context.parser().textOrNull();
        if (fieldType().indexed()) {
            fields.add(new StringField(context.parser().currentName(), value, Field.Store.NO));
        } else {
            context.ignoredValue(context.parser().currentName(), value);
        }
        Analyzer analyzer = context.docMapper().mappers().indexAnalyzer();
        if (value != null) {
            analyzer = context.analysisService().analyzer(value);
            if (analyzer == null) {
                throw new MapperParsingException("No analyzer found for [" + value + "] from path [" + path + "]");
            }
            analyzer = context.docMapper().mappers().indexAnalyzer(analyzer);
        }
        context.analyzer(analyzer);
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        if (path.equals(Defaults.PATH) && fieldType.indexed() == Defaults.FIELD_TYPE.indexed() &&
                fieldType.stored() == Defaults.FIELD_TYPE.stored() && !includeDefaults) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (includeDefaults || !path.equals(Defaults.PATH)) {
            builder.field("path", path);
        }
        if (includeDefaults || !(fieldType.indexed() == Defaults.FIELD_TYPE.indexed() &&
                fieldType.stored() == Defaults.FIELD_TYPE.stored())) {
            builder.field("index", indexTokenizeOptionToString(fieldType.indexed(), fieldType.tokenized()));
        }
        builder.endObject();
        return builder;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
