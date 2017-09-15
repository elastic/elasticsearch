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

import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.isArray;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeFloatValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeMapValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

public class TypeParsers {

    public static final String DOC_VALUES = "doc_values";
    public static final String INDEX_OPTIONS_DOCS = "docs";
    public static final String INDEX_OPTIONS_FREQS = "freqs";
    public static final String INDEX_OPTIONS_POSITIONS = "positions";
    public static final String INDEX_OPTIONS_OFFSETS = "offsets";

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(TypeParsers.class));

    //TODO 22298: Remove this method and have all call-sites use <code>XContentMapValues.nodeBooleanValue(node)</code> directly.
    public static boolean nodeBooleanValue(String fieldName, String propertyName, Object node,
                                           Mapper.TypeParser.ParserContext parserContext) {
        if (parserContext.indexVersionCreated().onOrAfter(Version.V_6_0_0_alpha1)) {
            return XContentMapValues.nodeBooleanValue(node, fieldName + "." + propertyName);
        } else {
            return nodeBooleanValueLenient(fieldName, propertyName, node);
        }
    }

    //TODO 22298: Remove this method and have all call-sites use <code>XContentMapValues.nodeBooleanValue(node)</code> directly.
    public static boolean nodeBooleanValueLenient(String fieldName, String propertyName, Object node) {
        if (Booleans.isBoolean(node.toString()) == false) {
            DEPRECATION_LOGGER.deprecated("Expected a boolean for property [{}] for field [{}] but got [{}]",
                propertyName, fieldName, node);
        }
        if (node instanceof Boolean) {
            return (Boolean) node;
        }
        if (node instanceof Number) {
            return ((Number) node).intValue() != 0;
        }
        @SuppressWarnings("deprecated")
        boolean value = Booleans.parseBooleanLenient(node.toString(), false);
        return value;
    }


    private static void parseAnalyzersAndTermVectors(FieldMapper.Builder builder, String name, Map<String, Object> fieldNode,
                                                     Mapper.TypeParser.ParserContext parserContext) {
        NamedAnalyzer indexAnalyzer = null;
        NamedAnalyzer searchAnalyzer = null;
        NamedAnalyzer searchQuoteAnalyzer = null;

        for (Iterator<Map.Entry<String, Object>> iterator = fieldNode.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, Object> entry = iterator.next();
            final String propName = entry.getKey();
            final Object propNode = entry.getValue();
            if (propName.equals("term_vector")) {
                parseTermVector(name, propNode.toString(), builder);
                iterator.remove();
            } else if (propName.equals("store_term_vectors")) {
                builder.storeTermVectors(nodeBooleanValue(name, "store_term_vectors", propNode, parserContext));
                iterator.remove();
            } else if (propName.equals("store_term_vector_offsets")) {
                builder.storeTermVectorOffsets(nodeBooleanValue(name, "store_term_vector_offsets", propNode, parserContext));
                iterator.remove();
            } else if (propName.equals("store_term_vector_positions")) {
                builder.storeTermVectorPositions(
                    nodeBooleanValue(name, "store_term_vector_positions", propNode, parserContext));
                iterator.remove();
            } else if (propName.equals("store_term_vector_payloads")) {
                builder.storeTermVectorPayloads(nodeBooleanValue(name,"store_term_vector_payloads", propNode, parserContext));
                iterator.remove();
            } else if (propName.equals("analyzer")) {
                NamedAnalyzer analyzer = parserContext.getIndexAnalyzers().get(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                indexAnalyzer = analyzer;
                iterator.remove();
            } else if (propName.equals("search_analyzer")) {
                NamedAnalyzer analyzer = parserContext.getIndexAnalyzers().get(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                searchAnalyzer = analyzer;
                iterator.remove();
            } else if (propName.equals("search_quote_analyzer")) {
                NamedAnalyzer analyzer = parserContext.getIndexAnalyzers().get(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                searchQuoteAnalyzer = analyzer;
                iterator.remove();
            }
        }

        if (indexAnalyzer == null && searchAnalyzer != null) {
            throw new MapperParsingException("analyzer on field [" + name + "] must be set when search_analyzer is set");
        }

        if (searchAnalyzer == null && searchQuoteAnalyzer != null) {
            throw new MapperParsingException("analyzer and search_analyzer on field [" + name +
                "] must be set when search_quote_analyzer is set");
        }

        if (searchAnalyzer == null) {
            searchAnalyzer = indexAnalyzer;
        }

        if (searchQuoteAnalyzer == null) {
            searchQuoteAnalyzer = searchAnalyzer;
        }

        if (indexAnalyzer != null) {
            builder.indexAnalyzer(indexAnalyzer);
        }
        if (searchAnalyzer != null) {
            builder.searchAnalyzer(searchAnalyzer);
        }
        if (searchQuoteAnalyzer != null) {
            builder.searchQuoteAnalyzer(searchQuoteAnalyzer);
        }
    }

    public static boolean parseNorms(FieldMapper.Builder builder, String fieldName, String propName, Object propNode,
                                     Mapper.TypeParser.ParserContext parserContext) {
        if (propName.equals("norms")) {
            if (propNode instanceof Map) {
                final Map<String, Object> properties = nodeMapValue(propNode, "norms");
                for (Iterator<Entry<String, Object>> propsIterator = properties.entrySet().iterator(); propsIterator.hasNext(); ) {
                    Entry<String, Object> entry2 = propsIterator.next();
                    final String propName2 = entry2.getKey();
                    final Object propNode2 = entry2.getValue();
                    if (propName2.equals("enabled")) {
                        builder.omitNorms(nodeBooleanValue(fieldName, "enabled", propNode2, parserContext) == false);
                        propsIterator.remove();
                    } else if (propName2.equals("loading")) {
                        // ignore for bw compat
                        propsIterator.remove();
                    }
                }
                DocumentMapperParser.checkNoRemainingFields(propName, properties, parserContext.indexVersionCreated());
                DEPRECATION_LOGGER.deprecated("The [norms{enabled:true/false}] way of specifying norms is deprecated, please use " +
                    "[norms:true/false] instead");
            } else {
                builder.omitNorms(nodeBooleanValue(fieldName,"norms", propNode, parserContext) == false);
            }
            return true;
        } else if (propName.equals("omit_norms")) {
            builder.omitNorms(nodeBooleanValue(fieldName,"norms", propNode, parserContext));
            DEPRECATION_LOGGER.deprecated("[omit_norms] is deprecated, please use [norms] instead with the opposite boolean value");
            return true;
        } else {
            return false;
        }
    }

    /**
     * Parse text field attributes. In addition to {@link #parseField common attributes}
     * this will parse analysis and term-vectors related settings.
     */
    public static void parseTextField(FieldMapper.Builder builder, String name, Map<String, Object> fieldNode,
                                      Mapper.TypeParser.ParserContext parserContext) {
        parseField(builder, name, fieldNode, parserContext);
        parseAnalyzersAndTermVectors(builder, name, fieldNode, parserContext);
        for (Iterator<Map.Entry<String, Object>> iterator = fieldNode.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, Object> entry = iterator.next();
            final String propName = entry.getKey();
            final Object propNode = entry.getValue();
            if (parseNorms(builder, name, propName, propNode, parserContext)) {
                iterator.remove();
            }
        }
    }

    /**
     * Parse common field attributes such as {@code doc_values} or {@code store}.
     */
    public static void parseField(FieldMapper.Builder builder, String name, Map<String, Object> fieldNode,
                                  Mapper.TypeParser.ParserContext parserContext) {
        for (Iterator<Map.Entry<String, Object>> iterator = fieldNode.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, Object> entry = iterator.next();
            final String propName = entry.getKey();
            final Object propNode = entry.getValue();
            if (false == propName.equals("null_value") && propNode == null) {
                /*
                 * No properties *except* null_value are allowed to have null. So we catch it here and tell the user something useful rather
                 * than send them a null pointer exception later.
                 */
                throw new MapperParsingException("[" + propName + "] must not have a [null] value");
            }
            if (propName.equals("store")) {
                builder.store(nodeBooleanValue(name,"store", propNode.toString(), parserContext));
                iterator.remove();
            } else if (propName.equals("index")) {
                builder.index(nodeBooleanValue(name, "index", propNode, parserContext));
                iterator.remove();
            } else if (propName.equals(DOC_VALUES)) {
                builder.docValues(nodeBooleanValue(name, DOC_VALUES, propNode, parserContext));
                iterator.remove();
            } else if (propName.equals("boost")) {
                builder.boost(nodeFloatValue(propNode));
                iterator.remove();
            } else if (parserContext.indexVersionCreated().before(Version.V_5_0_0_alpha1)
                && parseNorms(builder, name, propName, propNode, parserContext)) {
                iterator.remove();
            } else if (propName.equals("index_options")) {
                if (builder.allowsIndexOptions()) {
                    builder.indexOptions(nodeIndexOptionValue(propNode));
                } else {
                    DEPRECATION_LOGGER.deprecated(
                            "index_options are deprecated for field [{}] of type [{}] and will be removed in the next major version.",
                            name, builder.fieldType.typeName());
                }
                iterator.remove();
            } else if (propName.equals("include_in_all")) {
                if (parserContext.isWithinMultiField()) {
                    throw new MapperParsingException("include_in_all in multi fields is not allowed. Found the include_in_all in field ["
                        + name + "] which is within a multi field.");
                } else if (parserContext.indexVersionCreated().onOrAfter(Version.V_6_0_0_alpha1)) {
                    throw new MapperParsingException("[include_in_all] is not allowed for indices created on or after version 6.0.0 as " +
                                    "[_all] is deprecated. As a replacement, you can use an [copy_to] on mapping fields to create your " +
                                    "own catch all field.");
                } else {
                    builder.includeInAll(nodeBooleanValue(name, "include_in_all", propNode, parserContext));
                }
                iterator.remove();
            } else if (propName.equals("similarity")) {
                SimilarityProvider similarityProvider = resolveSimilarity(parserContext, name, propNode.toString());
                builder.similarity(similarityProvider);
                iterator.remove();
            } else if (propName.equals("fielddata")
                    && propNode instanceof Map
                    && parserContext.indexVersionCreated().before(Version.V_5_0_0_alpha1)) {
                // ignore for bw compat
                iterator.remove();
            } else if (parseMultiField(builder, name, parserContext, propName, propNode)) {
                iterator.remove();
            } else if (propName.equals("copy_to")) {
                if (parserContext.isWithinMultiField()) {
                    throw new MapperParsingException("copy_to in multi fields is not allowed. Found the copy_to in field [" + name + "] " +
                        "which is within a multi field.");
                } else {
                    parseCopyFields(propNode, builder);
                }
                iterator.remove();
            }
        }
    }

    public static boolean parseMultiField(FieldMapper.Builder builder, String name, Mapper.TypeParser.ParserContext parserContext,
                                          String propName, Object propNode) {
        parserContext = parserContext.createMultiFieldContext(parserContext);
        if (propName.equals("fields")) {

            final Map<String, Object> multiFieldsPropNodes;

            if (propNode instanceof List && ((List<?>) propNode).isEmpty()) {
                multiFieldsPropNodes = Collections.emptyMap();
            } else if (propNode instanceof Map) {
                multiFieldsPropNodes = (Map<String, Object>) propNode;
            } else {
                throw new MapperParsingException("expected map for property [fields] on field [" + propNode + "] or " +
                    "[" + propName + "] but got a " + propNode.getClass());
            }

            for (Map.Entry<String, Object> multiFieldEntry : multiFieldsPropNodes.entrySet()) {
                String multiFieldName = multiFieldEntry.getKey();
                if (multiFieldName.contains(".")) {
                    throw new MapperParsingException("Field name [" + multiFieldName + "] which is a multi field of [" + name + "] cannot" +
                        " contain '.'");
                }
                if (!(multiFieldEntry.getValue() instanceof Map)) {
                    throw new MapperParsingException("illegal field [" + multiFieldName + "], only fields can be specified inside fields");
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> multiFieldNodes = (Map<String, Object>) multiFieldEntry.getValue();

                String type;
                Object typeNode = multiFieldNodes.get("type");
                if (typeNode != null) {
                    type = typeNode.toString();
                } else {
                    throw new MapperParsingException("no type specified for property [" + multiFieldName + "]");
                }
                if (type.equals(ObjectMapper.CONTENT_TYPE) || type.equals(ObjectMapper.NESTED_CONTENT_TYPE)) {
                    throw new MapperParsingException("Type [" + type + "] cannot be used in multi field");
                }

                Mapper.TypeParser typeParser = parserContext.typeParser(type);
                if (typeParser == null) {
                    throw new MapperParsingException("no handler for type [" + type + "] declared on field [" + multiFieldName + "]");
                }
                builder.addMultiField(typeParser.parse(multiFieldName, multiFieldNodes, parserContext));
                multiFieldNodes.remove("type");
                DocumentMapperParser.checkNoRemainingFields(propName, multiFieldNodes, parserContext.indexVersionCreated());
            }
            return true;
        }
        return false;
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
            return IndexOptions.DOCS;
        } else {
            throw new ElasticsearchParseException("failed to parse index option [{}]", value);
        }
    }

    public static FormatDateTimeFormatter parseDateTimeFormatter(Object node) {
        return Joda.forPattern(node.toString());
    }

    public static void parseTermVector(String fieldName, String termVector, FieldMapper.Builder builder) throws MapperParsingException {
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
            throw new MapperParsingException("wrong value for termVector [" + termVector + "] for field [" + fieldName + "]");
        }
    }

    @SuppressWarnings("unchecked")
    public static void parseCopyFields(Object propNode, FieldMapper.Builder builder) {
        FieldMapper.CopyTo.Builder copyToBuilder = new FieldMapper.CopyTo.Builder();
        if (isArray(propNode)) {
            for (Object node : (List<Object>) propNode) {
                copyToBuilder.add(nodeStringValue(node, null));
            }
        } else {
            copyToBuilder.add(nodeStringValue(propNode, null));
        }
        builder.copyTo(copyToBuilder.build());
    }

    private static SimilarityProvider resolveSimilarity(Mapper.TypeParser.ParserContext parserContext, String name, String value) {
        if (parserContext.indexVersionCreated().before(Version.V_5_0_0_alpha1) && "default".equals(value)) {
            // "default" similarity has been renamed into "classic" in 3.x.
            value = "classic";
        }
        SimilarityProvider similarityProvider = parserContext.getSimilarity(value);
        if (similarityProvider == null) {
            throw new MapperParsingException("Unknown Similarity type [" + value + "] for field [" + name + "]");
        }
        return similarityProvider;
    }
}
