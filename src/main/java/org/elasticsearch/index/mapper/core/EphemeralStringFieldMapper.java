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

package org.elasticsearch.index.mapper.core;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.ephemeralStringField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;

/**
 * This class extends StringFieldMapper only to the extent that it makes it into an auto field
 * 
 * However, in order to do this, we MUST have local copies of the Builder and TypeParser
 * with all references to the parent class modified appropriately.
 * - NO OTHER CHANGES ARE REQUIRED IN THESE LOCAL COPIES
 * 
 */
public class EphemeralStringFieldMapper extends StringFieldMapper {
    
    public static final String CONTENT_TYPE = "ephemeral_string";


    
    public static class Builder extends AbstractFieldMapper.Builder<Builder, EphemeralStringFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        protected int positionOffsetGap = Defaults.POSITION_OFFSET_GAP;

        protected NamedAnalyzer searchQuotedAnalyzer;

        protected int ignoreAbove = Defaults.IGNORE_ABOVE;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
            builder = this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public Builder searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            super.searchAnalyzer(searchAnalyzer);
            if (searchQuotedAnalyzer == null) {
                searchQuotedAnalyzer = searchAnalyzer;
            }
            return this;
        }

        public Builder positionOffsetGap(int positionOffsetGap) {
            this.positionOffsetGap = positionOffsetGap;
            return this;
        }

        public Builder searchQuotedAnalyzer(NamedAnalyzer analyzer) {
            this.searchQuotedAnalyzer = analyzer;
            return builder;
        }

        public Builder ignoreAbove(int ignoreAbove) {
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        @Override
        public EphemeralStringFieldMapper build(BuilderContext context) {
            if (positionOffsetGap > 0) {
                indexAnalyzer = new NamedAnalyzer(indexAnalyzer, positionOffsetGap);
                searchAnalyzer = new NamedAnalyzer(searchAnalyzer, positionOffsetGap);
                searchQuotedAnalyzer = new NamedAnalyzer(searchQuotedAnalyzer, positionOffsetGap);
            }
            // if the field is not analyzed, then by default, we should omit norms and have docs only
            // index options, as probably what the user really wants
            // if they are set explicitly, we will use those values
            // we also change the values on the default field type so that toXContent emits what
            // differs from the defaults
            FieldType defaultFieldType = new FieldType(Defaults.FIELD_TYPE);
            if (fieldType.indexOptions() != IndexOptions.NONE && !fieldType.tokenized()) {
                defaultFieldType.setOmitNorms(true);
                defaultFieldType.setIndexOptions(IndexOptions.DOCS);
                if (!omitNormsSet && boost == Defaults.BOOST) {
                    fieldType.setOmitNorms(true);
                }
                if (!indexOptionsSet) {
                    fieldType.setIndexOptions(IndexOptions.DOCS);
                }
            }
            defaultFieldType.freeze();
            EphemeralStringFieldMapper fieldMapper = new EphemeralStringFieldMapper(buildNames(context),
                    boost, fieldType, defaultFieldType, docValues, nullValue, indexAnalyzer, searchAnalyzer, searchQuotedAnalyzer,
                    positionOffsetGap, ignoreAbove, postingsProvider, docValuesProvider, similarity, normsLoading, 
                    fieldDataSettings, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }
    
    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            EphemeralStringFieldMapper.Builder builder = ephemeralStringField(name);
            parseField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(propNode.toString());
                } else if (propName.equals("search_quote_analyzer")) {
                    NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                    if (analyzer == null) {
                        throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                    }
                    builder.searchQuotedAnalyzer(analyzer);
                } else if (propName.equals("position_offset_gap")) {
                    builder.positionOffsetGap(XContentMapValues.nodeIntegerValue(propNode, -1));
                    // we need to update to actual analyzers if they are not set in this case...
                    // so we can inject the position offset gap...
                    if (builder.indexAnalyzer == null) {
                        builder.indexAnalyzer = parserContext.analysisService().defaultIndexAnalyzer();
                    }
                    if (builder.searchAnalyzer == null) {
                        builder.searchAnalyzer = parserContext.analysisService().defaultSearchAnalyzer();
                    }
                    if (builder.searchQuotedAnalyzer == null) {
                        builder.searchQuotedAnalyzer = parserContext.analysisService().defaultSearchQuoteAnalyzer();
                    }
                } else if (propName.equals("ignore_above")) {
                    builder.ignoreAbove(XContentMapValues.nodeIntegerValue(propNode, -1));
                } else {
                    parseMultiField(builder, name, parserContext, propName, propNode);
                }
            }
            return builder;
        }
    }
    
    // Just pass all the work to super
    protected EphemeralStringFieldMapper(org.elasticsearch.index.mapper.FieldMapper.Names names, float boost, FieldType fieldType,
            FieldType defaultFieldType, Boolean docValues, String nullValue, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer,
            NamedAnalyzer searchQuotedAnalyzer, int positionOffsetGap, int ignoreAbove, PostingsFormatProvider postingsFormat,
            DocValuesFormatProvider docValuesFormat, SimilarityProvider similarity,
            org.elasticsearch.index.mapper.FieldMapper.Loading normsLoading, Settings fieldDataSettings, Settings indexSettings,
            org.elasticsearch.index.mapper.core.AbstractFieldMapper.MultiFields multiFields,
            org.elasticsearch.index.mapper.core.AbstractFieldMapper.CopyTo copyTo) {
        super(names, boost, fieldType, defaultFieldType, docValues, nullValue, indexAnalyzer, searchAnalyzer, searchQuotedAnalyzer,
                positionOffsetGap, ignoreAbove, postingsFormat, docValuesFormat, similarity, normsLoading, fieldDataSettings, indexSettings,
                multiFields, copyTo);
    }

    @Override
    protected String contentType() { return CONTENT_TYPE; }

    @Override
    public boolean isEphemeral() { return true; }
}
