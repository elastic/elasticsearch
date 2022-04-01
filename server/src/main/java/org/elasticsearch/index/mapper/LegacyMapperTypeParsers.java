/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class LegacyMapperTypeParsers {

    private static final Logger logger = LogManager.getLogger(LegacyMapperTypeParsers.class);

    public static final LegacyMapperTypeParsers INSTANCE = new LegacyMapperTypeParsers();   // TODO make this pluggable?

    private LegacyMapperTypeParsers() {}

    public Mapper.TypeParser getParser(String fieldType) {
        return switch (fieldType) {
            case "boolean" -> BOOLEAN;
            case "date", "date_nanos" -> DATE;
            case "half_float", "float", "double", "byte", "short", "int", "long" -> NUMBER;
            case "geo_point" -> GEO_POINT;
            case "ip" -> IP;
            case "keyword" -> KEYWORD;
            default -> PLACEHOLDER;
        };
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> meta(Map<String, Object> node) {
        if (node.containsKey("meta") == false) {
            return Collections.emptyMap();
        }
        Object meta = node.get("meta");
        if (meta instanceof Map<?,?> == false) {
            throw new MapperParsingException("meta parameter must be a map, but found" + meta);
        }
        return (Map<String, String>) meta;
    }

    private static final LegacyTypeParser KEYWORD = new LegacyTypeParser() {
        @Override
        protected MappedFieldType doBuildMappedFieldType(String name, Map<String, Object> node, MappingParserContext context) {
            NamedAnalyzer analyzer = Lucene.KEYWORD_ANALYZER;
            String normalizer = XContentMapValues.nodeStringValue(node.get("normalizer"), "default");
            if (Objects.equals("normalizer", "default") == false) {
                analyzer = context.getIndexAnalyzers().getNormalizer(normalizer);
                if (analyzer == null) {
                    logger.warn(
                        new ParameterizedMessage(
                            "Could not find normalizer [{}] of legacy index, falling back to default",
                            normalizer
                        )
                    );
                    analyzer = Lucene.KEYWORD_ANALYZER;
                }
            }
            return new KeywordFieldMapper.KeywordFieldType(name, false, analyzer, meta(node));
        }
    };

    private static final LegacyTypeParser IP = new LegacyTypeParser() {
        @Override
        protected MappedFieldType doBuildMappedFieldType(String name, Map<String, Object> node, MappingParserContext context) {
            return new IpFieldMapper.IpFieldType(name, meta(node));
        }
    };

    private static final LegacyTypeParser BOOLEAN = new LegacyTypeParser() {
        @Override
        protected MappedFieldType doBuildMappedFieldType(String name, Map<String, Object> node, MappingParserContext context) {
            return new BooleanFieldMapper.BooleanFieldType(name, meta(node));
        }
    };

    private static final LegacyTypeParser DATE = new LegacyTypeParser() {
        @Override
        protected MappedFieldType doBuildMappedFieldType(String name, Map<String, Object> node, MappingParserContext context) {
            DateFieldMapper.Resolution resolution = DateFieldMapper.Resolution.MILLISECONDS;
            if (Objects.equals("date_nanos", node.get("type"))) {
                resolution = DateFieldMapper.Resolution.NANOSECONDS;
            }
            DateFormatter dateFormatter = context.getDateFormatter();
            if (dateFormatter == null) {
                dateFormatter = resolution == DateFieldMapper.Resolution.NANOSECONDS ?
                    DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER :
                    DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
            }
            String format = XContentMapValues.nodeStringValue(node.get("format"));
            String locale = XContentMapValues.nodeStringValue(node.get("locale"));
            if (format != null || locale != null) {
                format = format == null ? dateFormatter.pattern() : format;
                Locale l = locale == null ? dateFormatter.locale() : LocaleUtils.parse(locale);
                try {
                    dateFormatter = DateFormatter.forPattern(format).withLocale(l);
                } catch (IllegalArgumentException e) {
                    logger.warn(
                        new ParameterizedMessage("Error parsing format [{}] of legacy index, falling back to default", format),
                        e
                    );
                }
            }
            return new DateFieldMapper.DateFieldType(name, resolution, dateFormatter, meta(node));
        }
    };

    private static final LegacyTypeParser NUMBER = new LegacyTypeParser() {
        @Override
        protected MappedFieldType doBuildMappedFieldType(String name, Map<String, Object> node, MappingParserContext context) {
            String t = (String) node.get("type");
            NumberFieldMapper.NumberType type = NumberFieldMapper.NumberType.valueOf(t.toUpperCase(Locale.ROOT));
            return new NumberFieldMapper.NumberFieldType(name, type, meta(node));
        }
    };

    private static final LegacyTypeParser GEO_POINT = new LegacyTypeParser() {
        @Override
        protected MappedFieldType doBuildMappedFieldType(String name, Map<String, Object> node, MappingParserContext context) {
            return new GeoPointFieldMapper.GeoPointFieldType(name, meta(node));
        }
    };

    private static final LegacyTypeParser PLACEHOLDER = new LegacyTypeParser() {
        @Override
        protected MappedFieldType doBuildMappedFieldType(String name, Map<String, Object> node, MappingParserContext context) {
            return new PlaceHolderFieldMapper.PlaceHolderFieldType(name, (String) node.get("type"), meta(node));
        }
    };

    abstract static class LegacyTypeParser implements Mapper.TypeParser {

        @Override
        public final Mapper.Builder parse(
            String name,
            Map<String, Object> node,
            MappingParserContext parserContext
        ) throws MapperParsingException {
            FieldMapper.MultiFields.Builder multiFieldsBuilder = new FieldMapper.MultiFields.Builder();
            if (node.containsKey("fields")) {
                TypeParsers.parseMultiField(multiFieldsBuilder::add, name, parserContext, "fields", node.get("fields"));
                node.remove("fields");
            }
            Map<String, Object> nodeCopy = Map.copyOf(node);
            node.clear();   // we may ignore some params during parsing of legacy mappings, so we just clear everything here
            return new Mapper.Builder(name) {
                @Override
                public Mapper build(MapperBuilderContext context) {
                    FieldMapper.MultiFields multiFields = multiFieldsBuilder.build(this, context);
                    MappedFieldType fieldType = buildMappedFieldType(context.buildFullName(name), nodeCopy, parserContext);
                    return new LegacyFieldMapper(name, fieldType, multiFields, nodeCopy);
                }
            };
        }

        protected final MappedFieldType buildMappedFieldType(String name, Map<String, Object> node, MappingParserContext context) {
            if (XContentMapValues.nodeBooleanValue(node.get("doc_values"), "doc_values", true) == false) {
                return new PlaceHolderFieldMapper.PlaceHolderFieldType(name, (String) node.get("type"), meta(node));
            }
            return doBuildMappedFieldType(name, node, context);
        }

        protected abstract MappedFieldType doBuildMappedFieldType(String name, Map<String, Object> node, MappingParserContext context);
    }

    private static class LegacyFieldMapper extends FieldMapper {

        final Map<String, Object> params;

        protected LegacyFieldMapper(
            String simpleName,
            MappedFieldType mappedFieldType,
            MultiFields multiFields,
            Map<String, Object> params
        ) {
            super(simpleName, mappedFieldType, multiFields, CopyTo.empty());    // TODO do we need copy_to for value fetching?
            this.params = params;
        }

        @Override
        protected void parseCreateField(DocumentParserContext context) throws IOException {
            throw new UnsupportedOperationException("Legacy field mappers to not support indexing");
        }

        @Override
        public Builder getMergeBuilder() {
            throw new UnsupportedOperationException("Legacy field mappers do not support merging");
        }

        @Override
        protected String contentType() {
            return (String) params.get("type");
        }

        @Override
        protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
            for (var entry : this.params.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            multiFields.toXContent(builder, params);
        }
    }

}
