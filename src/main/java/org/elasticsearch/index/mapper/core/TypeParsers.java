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

import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper.Loading;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*;
import static org.elasticsearch.index.mapper.FieldMapper.DOC_VALUES_FORMAT;

/**
 *
 */
public class TypeParsers {

    public static final String MULTI_FIELD_CONTENT_TYPE = "multi_field";
    public static final Mapper.TypeParser multiFieldConverterTypeParser = new Mapper.TypeParser() {

        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            ContentPath.Type pathType = null;
            AbstractFieldMapper.Builder mainFieldBuilder = null;
            List<AbstractFieldMapper.Builder> fields = null;
            String firstType = null;

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    pathType = parsePathType(name, fieldNode.toString());
                } else if (fieldName.equals("fields")) {
                    Map<String, Object> fieldsNode = (Map<String, Object>) fieldNode;
                    for (Map.Entry<String, Object> entry1 : fieldsNode.entrySet()) {
                        String propName = entry1.getKey();
                        Map<String, Object> propNode = (Map<String, Object>) entry1.getValue();

                        String type;
                        Object typeNode = propNode.get("type");
                        if (typeNode != null) {
                            type = typeNode.toString();
                            if (firstType == null) {
                                firstType = type;
                            }
                        } else {
                            throw new MapperParsingException("No type specified for property [" + propName + "]");
                        }

                        Mapper.TypeParser typeParser = parserContext.typeParser(type);
                        if (typeParser == null) {
                            throw new MapperParsingException("No handler for type [" + type + "] declared on field [" + fieldName + "]");
                        }
                        if (propName.equals(name)) {
                            mainFieldBuilder = (AbstractFieldMapper.Builder) typeParser.parse(propName, propNode, parserContext);
                        } else {
                            if (fields == null) {
                                fields = new ArrayList<>(2);
                            }
                            fields.add((AbstractFieldMapper.Builder) typeParser.parse(propName, propNode, parserContext));
                        }
                    }
                }
            }

            if (mainFieldBuilder == null) {
                if (fields == null) {
                    // No fields at all were specified in multi_field, so lets return a non indexed string field.
                    return new StringFieldMapper.Builder(name).index(false);
                }
                Mapper.TypeParser typeParser = parserContext.typeParser(firstType);
                if (typeParser == null) {
                    // The first multi field's type is unknown
                    mainFieldBuilder = new StringFieldMapper.Builder(name).index(false);
                } else {
                    Mapper.Builder substitute = typeParser.parse(name, Collections.<String, Object>emptyMap(), parserContext);
                    if (substitute instanceof AbstractFieldMapper.Builder) {
                        mainFieldBuilder = ((AbstractFieldMapper.Builder) substitute).index(false);
                    } else {
                        // The first multi isn't a core field type
                        mainFieldBuilder =  new StringFieldMapper.Builder(name).index(false);
                    }
                }
            }

            if (fields != null && pathType != null) {
                for (Mapper.Builder field : fields) {
                    mainFieldBuilder.addMultiField(field);
                }
                mainFieldBuilder.multiFieldPathType(pathType);
            } else if (fields != null) {
                for (Mapper.Builder field : fields) {
                    mainFieldBuilder.addMultiField(field);
                }
            } else if (pathType != null) {
                mainFieldBuilder.multiFieldPathType(pathType);
            }
            return mainFieldBuilder;
        }

    };

    public static final String DOC_VALUES = "doc_values";
    public static final String INDEX_OPTIONS_DOCS = "docs";
    public static final String INDEX_OPTIONS_FREQS = "freqs";
    public static final String INDEX_OPTIONS_POSITIONS = "positions";
    public static final String INDEX_OPTIONS_OFFSETS = "offsets";

    public static void parseNumberField(NumberFieldMapper.Builder builder, String name, Map<String, Object> numberNode, Mapper.TypeParser.ParserContext parserContext) {
        parseField(builder, name, numberNode, parserContext);
        for (Map.Entry<String, Object> entry : numberNode.entrySet()) {
            String propName = Strings.toUnderscoreCase(entry.getKey());
            Object propNode = entry.getValue();
            if (propName.equals("precision_step")) {
                builder.precisionStep(nodeIntegerValue(propNode));
            } else if (propName.equals("ignore_malformed")) {
                builder.ignoreMalformed(nodeBooleanValue(propNode));
            } else if (propName.equals("coerce")) {
                builder.coerce(nodeBooleanValue(propNode));
            } else if (propName.equals("omit_norms")) {
                builder.omitNorms(nodeBooleanValue(propNode));
            } else if (propName.equals("similarity")) {
                builder.similarity(parserContext.similarityLookupService().similarity(propNode.toString()));
            } else {
                parseMultiField(builder, name, numberNode, parserContext, propName, propNode);
            }
        }
    }

    public static void parseField(AbstractFieldMapper.Builder builder, String name, Map<String, Object> fieldNode, Mapper.TypeParser.ParserContext parserContext) {
        for (Map.Entry<String, Object> entry : fieldNode.entrySet()) {
            final String propName = Strings.toUnderscoreCase(entry.getKey());
            final Object propNode = entry.getValue();
            if (propName.equals("index_name")) {
                builder.indexName(propNode.toString());
            } else if (propName.equals("store")) {
                builder.store(parseStore(name, propNode.toString()));
            } else if (propName.equals("index")) {
                parseIndex(name, propNode.toString(), builder);
            } else if (propName.equals("tokenized")) {
                builder.tokenized(nodeBooleanValue(propNode));
            } else if (propName.equals(DOC_VALUES)) {
                builder.docValues(nodeBooleanValue(propNode));
            } else if (propName.equals("term_vector")) {
                parseTermVector(name, propNode.toString(), builder);
            } else if (propName.equals("boost")) {
                builder.boost(nodeFloatValue(propNode));
            } else if (propName.equals("store_term_vectors")) {
                builder.storeTermVectors(nodeBooleanValue(propNode));
            } else if (propName.equals("store_term_vector_offsets")) {
                builder.storeTermVectorOffsets(nodeBooleanValue(propNode));
            } else if (propName.equals("store_term_vector_positions")) {
                builder.storeTermVectorPositions(nodeBooleanValue(propNode));
            } else if (propName.equals("store_term_vector_payloads")) {
                builder.storeTermVectorPayloads(nodeBooleanValue(propNode));
            } else if (propName.equals("omit_norms")) {
                builder.omitNorms(nodeBooleanValue(propNode));
            } else if (propName.equals("norms")) {
                final Map<String, Object> properties = nodeMapValue(propNode, "norms");
                for (Map.Entry<String, Object> entry2 : properties.entrySet()) {
                    final String propName2 = Strings.toUnderscoreCase(entry2.getKey());
                    final Object propNode2 = entry2.getValue();
                    if (propName2.equals("enabled")) {
                        builder.omitNorms(!nodeBooleanValue(propNode2));
                    } else if (propName2.equals(Loading.KEY)) {
                        builder.normsLoading(Loading.parse(nodeStringValue(propNode2, null), null));
                    }
                }
            } else if (propName.equals("omit_term_freq_and_positions")) {
                final IndexOptions op = nodeBooleanValue(propNode) ? IndexOptions.DOCS_ONLY : IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
                if (parserContext.indexVersionCreated().onOrAfter(Version.V_1_0_0_RC2)) {
                    throw new ElasticsearchParseException("'omit_term_freq_and_positions' is not supported anymore - use ['index_options' : '" + op.name() + "']  instead");
                }
                // deprecated option for BW compat
                builder.indexOptions(op);
            } else if (propName.equals("index_options")) {
                builder.indexOptions(nodeIndexOptionValue(propNode));
            } else if (propName.equals("analyzer")) {
                NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                builder.indexAnalyzer(analyzer);
                builder.searchAnalyzer(analyzer);
            } else if (propName.equals("index_analyzer")) {
                NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                builder.indexAnalyzer(analyzer);
            } else if (propName.equals("search_analyzer")) {
                NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                builder.searchAnalyzer(analyzer);
            } else if (propName.equals("include_in_all")) {
                builder.includeInAll(nodeBooleanValue(propNode));
            } else if (propName.equals("postings_format")) {
                String postingFormatName = propNode.toString();
                builder.postingsFormat(parserContext.postingFormatService().get(postingFormatName));
            } else if (propName.equals(DOC_VALUES_FORMAT)) {
                String docValuesFormatName = propNode.toString();
                builder.docValuesFormat(parserContext.docValuesFormatService().get(docValuesFormatName));
            } else if (propName.equals("similarity")) {
                builder.similarity(parserContext.similarityLookupService().similarity(propNode.toString()));
            } else if (propName.equals("fielddata")) {
                final Settings settings = ImmutableSettings.builder().put(SettingsLoader.Helper.loadNestedFromMap(nodeMapValue(propNode, "fielddata"))).build();
                builder.fieldDataSettings(settings);
            } else if (propName.equals("copy_to")) {
                parseCopyFields(propNode, builder);
            }
        }
    }

    public static void parseMultiField(AbstractFieldMapper.Builder builder, String name, Map<String, Object> node, Mapper.TypeParser.ParserContext parserContext, String propName, Object propNode) {
        if (propName.equals("path")) {
            builder.multiFieldPathType(parsePathType(name, propNode.toString()));
        } else if (propName.equals("fields")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> multiFieldsPropNodes = (Map<String, Object>) propNode;
            for (Map.Entry<String, Object> multiFieldEntry : multiFieldsPropNodes.entrySet()) {
                String multiFieldName = multiFieldEntry.getKey();
                if (!(multiFieldEntry.getValue() instanceof Map)) {
                    throw new MapperParsingException("Illegal field [" + multiFieldName + "], only fields can be specified inside fields");
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> multiFieldNodes = (Map<String, Object>) multiFieldEntry.getValue();

                String type;
                Object typeNode = multiFieldNodes.get("type");
                if (typeNode != null) {
                    type = typeNode.toString();
                } else {
                    throw new MapperParsingException("No type specified for property [" + multiFieldName + "]");
                }

                Mapper.TypeParser typeParser = parserContext.typeParser(type);
                if (typeParser == null) {
                    throw new MapperParsingException("No handler for type [" + type + "] declared on field [" + multiFieldName + "]");
                }
                builder.addMultiField(typeParser.parse(multiFieldName, multiFieldNodes, parserContext));
            }
        }
    }

    private static IndexOptions nodeIndexOptionValue(final Object propNode) {
        final String value = propNode.toString();
        if (INDEX_OPTIONS_OFFSETS.equalsIgnoreCase(value)) {
            return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        } else if (INDEX_OPTIONS_POSITIONS.equalsIgnoreCase(value)) {
            return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        } else if (INDEX_OPTIONS_FREQS.equalsIgnoreCase(value)) {
            return IndexOptions.DOCS_AND_FREQS;
        } else if (INDEX_OPTIONS_DOCS.equalsIgnoreCase(value)) {
            return IndexOptions.DOCS_ONLY;
        } else {
            throw new ElasticsearchParseException("Failed to parse index option [" + value + "]");
        }
    }

    public static FormatDateTimeFormatter parseDateTimeFormatter(String fieldName, Object node) {
        return Joda.forPattern(node.toString());
    }

    public static void parseTermVector(String fieldName, String termVector, AbstractFieldMapper.Builder builder) throws MapperParsingException {
        termVector = Strings.toUnderscoreCase(termVector);
        if ("no".equals(termVector)) {
            builder.storeTermVectors(false);
        } else if ("yes".equals(termVector)) {
            builder.storeTermVectors(true);
        } else if ("with_offsets".equals(termVector)) {
            builder.storeTermVectorOffsets(true);
        } else if ("with_positions".equals(termVector)) {
            builder.storeTermVectorPositions(true);
        } else if ("with_positions_offsets".equals(termVector)) {
            builder.storeTermVectorPositions(true);
            builder.storeTermVectorOffsets(true);
        } else if ("with_positions_payloads".equals(termVector)) {
            builder.storeTermVectorPositions(true);
            builder.storeTermVectorPayloads(true);
        } else if ("with_positions_offsets_payloads".equals(termVector)) {
            builder.storeTermVectorPositions(true);
            builder.storeTermVectorOffsets(true);
            builder.storeTermVectorPayloads(true);
        } else {
            throw new MapperParsingException("Wrong value for termVector [" + termVector + "] for field [" + fieldName + "]");
        }
    }

    public static void parseIndex(String fieldName, String index, AbstractFieldMapper.Builder builder) throws MapperParsingException {
        index = Strings.toUnderscoreCase(index);
        if ("no".equals(index)) {
            builder.index(false);
        } else if ("not_analyzed".equals(index)) {
            builder.index(true);
            builder.tokenized(false);
        } else if ("analyzed".equals(index)) {
            builder.index(true);
            builder.tokenized(true);
        } else {
            throw new MapperParsingException("Wrong value for index [" + index + "] for field [" + fieldName + "]");
        }
    }

    public static boolean parseDocValues(String docValues) {
        if ("no".equals(docValues)) {
            return false;
        } else if ("yes".equals(docValues)) {
            return true;
        } else {
            return nodeBooleanValue(docValues);
        }
    }

    public static boolean parseStore(String fieldName, String store) throws MapperParsingException {
        if ("no".equals(store)) {
            return false;
        } else if ("yes".equals(store)) {
            return true;
        } else {
            return nodeBooleanValue(store);
        }
    }

    public static ContentPath.Type parsePathType(String name, String path) throws MapperParsingException {
        path = Strings.toUnderscoreCase(path);
        if ("just_name".equals(path)) {
            return ContentPath.Type.JUST_NAME;
        } else if ("full".equals(path)) {
            return ContentPath.Type.FULL;
        } else {
            throw new MapperParsingException("Wrong value for pathType [" + path + "] for object [" + name + "]");
        }
    }

    @SuppressWarnings("unchecked")
    public static void parseCopyFields(Object propNode, AbstractFieldMapper.Builder builder) {
        AbstractFieldMapper.CopyTo.Builder copyToBuilder = new AbstractFieldMapper.CopyTo.Builder();
        if (isArray(propNode)) {
            for(Object node : (List<Object>) propNode) {
                copyToBuilder.add(nodeStringValue(node, null));
            }
        } else {
            copyToBuilder.add(nodeStringValue(propNode, null));
        }
        builder.copyTo(copyToBuilder.build());
    }

}
