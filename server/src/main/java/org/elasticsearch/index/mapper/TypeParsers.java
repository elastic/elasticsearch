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
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.isArray;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeFloatValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

public class TypeParsers {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TypeParsers.class);

    public static final String DOC_VALUES = "doc_values";
    public static final String INDEX_OPTIONS_DOCS = "docs";
    public static final String INDEX_OPTIONS_FREQS = "freqs";
    public static final String INDEX_OPTIONS_POSITIONS = "positions";
    public static final String INDEX_OPTIONS_OFFSETS = "offsets";

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
                builder.storeTermVectors(XContentMapValues.nodeBooleanValue(propNode, name + ".store_term_vectors"));
                iterator.remove();
            } else if (propName.equals("store_term_vector_offsets")) {
                builder.storeTermVectorOffsets(XContentMapValues.nodeBooleanValue(propNode, name + ".store_term_vector_offsets"));
                iterator.remove();
            } else if (propName.equals("store_term_vector_positions")) {
                builder.storeTermVectorPositions(XContentMapValues.nodeBooleanValue(propNode, name + ".store_term_vector_positions"));
                iterator.remove();
            } else if (propName.equals("store_term_vector_payloads")) {
                builder.storeTermVectorPayloads(XContentMapValues.nodeBooleanValue(propNode, name + ".store_term_vector_payloads"));
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
                analyzer.checkAllowedInMode(AnalysisMode.SEARCH_TIME);
                searchAnalyzer = analyzer;
                iterator.remove();
            } else if (propName.equals("search_quote_analyzer")) {
                NamedAnalyzer analyzer = parserContext.getIndexAnalyzers().get(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                analyzer.checkAllowedInMode(AnalysisMode.SEARCH_TIME);
                searchQuoteAnalyzer = analyzer;
                iterator.remove();
            }
        }

        // check analyzers are allowed to work in the respective AnalysisMode
        {
            if (indexAnalyzer != null) {
                if (searchAnalyzer == null) {
                    indexAnalyzer.checkAllowedInMode(AnalysisMode.ALL);
                } else {
                    indexAnalyzer.checkAllowedInMode(AnalysisMode.INDEX_TIME);
                }
            }
            if (searchAnalyzer != null) {
                searchAnalyzer.checkAllowedInMode(AnalysisMode.SEARCH_TIME);
            }
            if (searchQuoteAnalyzer != null) {
                searchQuoteAnalyzer.checkAllowedInMode(AnalysisMode.SEARCH_TIME);
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

    public static void parseNorms(FieldMapper.Builder<?> builder, String fieldName, Object propNode) {
        builder.omitNorms(XContentMapValues.nodeBooleanValue(propNode, fieldName + ".norms") == false);
    }

    /**
     * Parse text field attributes. In addition to {@link #parseField common attributes}
     * this will parse analysis and term-vectors related settings.
     */
    public static void parseTextField(FieldMapper.Builder<?> builder, String name, Map<String, Object> fieldNode,
                                      Mapper.TypeParser.ParserContext parserContext) {
        parseField(builder, name, fieldNode, parserContext);
        parseAnalyzersAndTermVectors(builder, name, fieldNode, parserContext);
        for (Iterator<Map.Entry<String, Object>> iterator = fieldNode.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, Object> entry = iterator.next();
            final String propName = entry.getKey();
            final Object propNode = entry.getValue();
            if ("norms".equals(propName)) {
                parseNorms(builder, name, propNode);
                iterator.remove();
            }
        }
    }

    public static void checkNull(String propName, Object propNode) {
        if (false == propName.equals("null_value") && propNode == null) {
            /*
             * No properties *except* null_value are allowed to have null. So we catch it here and tell the user something useful rather
             * than send them a null pointer exception later.
             */
            throw new MapperParsingException("[" + propName + "] must not have a [null] value");
        }
    }

    /**
     * Parse the {@code meta} key of the mapping.
     */
    public static Map<String, String> parseMeta(String name, Object metaObject) {
        if (metaObject instanceof Map == false) {
            throw new MapperParsingException("[meta] must be an object, got " + metaObject.getClass().getSimpleName() +
                    "[" + metaObject + "] for field [" + name +"]");
        }
        @SuppressWarnings("unchecked")
        Map<String, ?> meta = (Map<String, ?>) metaObject;
        if (meta.size() > 5) {
            throw new MapperParsingException("[meta] can't have more than 5 entries, but got " + meta.size() + " on field [" +
                    name + "]");
        }
        for (String key : meta.keySet()) {
            if (key.codePointCount(0, key.length()) > 20) {
                throw new MapperParsingException("[meta] keys can't be longer than 20 chars, but got [" + key +
                        "] for field [" + name + "]");
            }
        }
        for (Object value : meta.values()) {
            if (value instanceof String) {
                String sValue = (String) value;
                if (sValue.codePointCount(0, sValue.length()) > 50) {
                    throw new MapperParsingException("[meta] values can't be longer than 50 chars, but got [" + value +
                            "] for field [" + name + "]");
                }
            } else if (value == null) {
                throw new MapperParsingException("[meta] values can't be null (field [" + name + "])");
            } else {
                throw new MapperParsingException("[meta] values can only be strings, but got " +
                        value.getClass().getSimpleName() + "[" + value + "] for field [" + name + "]");
            }
        }
        final Function<Map.Entry<String, ?>, Object> entryValueFunction = Map.Entry::getValue;
        final Function<Object, String> stringCast = String.class::cast;
        return meta.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entryValueFunction.andThen(stringCast)));
    }



    /**
     * Parse common field attributes such as {@code doc_values} or {@code store}.
     */
    public static void parseField(FieldMapper.Builder<?> builder, String name, Map<String, Object> fieldNode,
                                  Mapper.TypeParser.ParserContext parserContext) {
        for (Iterator<Map.Entry<String, Object>> iterator = fieldNode.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, Object> entry = iterator.next();
            final String propName = entry.getKey();
            final Object propNode = entry.getValue();
            checkNull(propName, propNode);
            if (propName.equals("store")) {
                builder.store(XContentMapValues.nodeBooleanValue(propNode, name + ".store"));
                iterator.remove();
            } else if (propName.equals("meta")) {
                builder.meta(parseMeta(name, propNode));
                iterator.remove();
            } else if (propName.equals("index")) {
                builder.index(XContentMapValues.nodeBooleanValue(propNode, name + ".index"));
                iterator.remove();
            } else if (propName.equals(DOC_VALUES)) {
                builder.docValues(XContentMapValues.nodeBooleanValue(propNode, name + "." + DOC_VALUES));
                iterator.remove();
            } else if (propName.equals("boost")) {
                builder.boost(nodeFloatValue(propNode));
                iterator.remove();
            } else if (propName.equals("index_options")) {
                builder.indexOptions(nodeIndexOptionValue(propNode));
                iterator.remove();
            } else if (propName.equals("similarity")) {
                deprecationLogger.deprecate("similarity",
                    "The [similarity] parameter has no effect on field [" + name + "] and will be removed in 8.0");
                iterator.remove();
            } else if (parseMultiField(builder::addMultiField, name, parserContext, propName, propNode)) {
                iterator.remove();
            } else if (propName.equals("copy_to")) {
                if (parserContext.isWithinMultiField()) {
                    throw new MapperParsingException("copy_to in multi fields is not allowed. Found the copy_to in field [" + name + "] " +
                        "which is within a multi field.");
                } else {
                    List<String> copyFields = parseCopyFields(propNode);
                    FieldMapper.CopyTo.Builder cpBuilder = new FieldMapper.CopyTo.Builder();
                    copyFields.forEach(cpBuilder::add);
                    builder.copyTo(cpBuilder.build());
                }
                iterator.remove();
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static boolean parseMultiField(Consumer<Mapper.Builder> multiFieldsBuilder, String name,
                                          Mapper.TypeParser.ParserContext parserContext, String propName, Object propNode) {
        if (propName.equals("fields")) {
            if (parserContext.isWithinMultiField()) {
                // For indices created prior to 8.0, we only emit a deprecation warning and do not fail type parsing. This is to
                // maintain the backwards-compatibility guarantee that we can always load indexes from the previous major version.
                if (parserContext.indexVersionCreated().before(Version.V_8_0_0)) {
                    deprecationLogger.deprecate("multifield_within_multifield", "At least one multi-field, [" + name + "], " +
                        "was encountered that itself contains a multi-field. Defining multi-fields within a multi-field is deprecated " +
                        "and is not supported for indices created in 8.0 and later. To migrate the mappings, all instances of [fields] " +
                        "that occur within a [fields] block should be removed from the mappings, either by flattening the chained " +
                        "[fields] blocks into a single level, or switching to [copy_to] if appropriate.");
                } else {
                    throw new IllegalArgumentException("Encountered a multi-field [" + name + "] which itself contains a multi-field. " +
                        "Defining chained multi-fields is not supported.");
                }
            }

            parserContext = parserContext.createMultiFieldContext(parserContext);

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
                Map<String, Object> multiFieldNodes = (Map<String, Object>) multiFieldEntry.getValue();

                String type;
                Object typeNode = multiFieldNodes.get("type");
                if (typeNode != null) {
                    type = typeNode.toString();
                } else {
                    throw new MapperParsingException("no type specified for property [" + multiFieldName + "]");
                }
                if (type.equals(ObjectMapper.CONTENT_TYPE)
                        || type.equals(ObjectMapper.NESTED_CONTENT_TYPE)
                        || type.equals(FieldAliasMapper.CONTENT_TYPE)) {
                    throw new MapperParsingException("Type [" + type + "] cannot be used in multi field");
                }

                Mapper.TypeParser typeParser = parserContext.typeParser(type);
                if (typeParser == null) {
                    throw new MapperParsingException("no handler for type [" + type + "] declared on field [" + multiFieldName + "]");
                }
                multiFieldsBuilder.accept(typeParser.parse(multiFieldName, multiFieldNodes, parserContext));
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

    public static DateFormatter parseDateTimeFormatter(Object node) {
        if (node instanceof String) {
            return DateFormatter.forPattern((String) node);
        }
        throw new IllegalArgumentException("Invalid format: [" + node.toString() + "]: expected string value");
    }

    @SuppressWarnings("rawtypes")
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

    public static List<String> parseCopyFields(Object propNode) {
        List<String> copyFields = new ArrayList<>();
        if (isArray(propNode)) {
            for (Object node : (List<Object>) propNode) {
                copyFields.add(nodeStringValue(node, null));
            }
        } else {
            copyFields.add(nodeStringValue(propNode, null));
        }
        return copyFields;
    }

    public static SimilarityProvider resolveSimilarity(Mapper.TypeParser.ParserContext parserContext, String name, String value) {
        SimilarityProvider similarityProvider = parserContext.getSimilarity(value);
        if (similarityProvider == null) {
            throw new MapperParsingException("Unknown Similarity type [" + value + "] for field [" + name + "]");
        }
        return similarityProvider;
    }
}
