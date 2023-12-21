/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

public class PassthroughObjectMapper extends ObjectMapper {
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(PassthroughObjectMapper.class);

    public static final String CONTENT_TYPE = "passthrough";

    public static class Builder extends ObjectMapper.Builder {

        protected Explicit<Boolean> containsDimensions = Explicit.IMPLICIT_FALSE;

        public Builder(String name) {
            super(name, Explicit.IMPLICIT_FALSE);
        }

        @Override
        public PassthroughObjectMapper.Builder add(Mapper.Builder builder) {
            if (containsDimensions.value() && builder instanceof KeywordFieldMapper.Builder keywordBuilder) {
                keywordBuilder.dimension(true);
            }
            super.add(builder);
            return this;
        }

        @Override
        public PassthroughObjectMapper build(MapperBuilderContext context) {
            return new PassthroughObjectMapper(
                name,
                enabled,
                subobjects,
                dynamic,
                buildMappers(context.createChildContext(name)),
                containsDimensions
            );
        }
    }

    private final Explicit<Boolean> containsDimensions;

    PassthroughObjectMapper(
        String name,
        Explicit<Boolean> enabled,
        Explicit<Boolean> subobjects,
        Dynamic dynamic,
        Map<String, Mapper> mappers,
        Explicit<Boolean> containsDimensions
    ) {
        super(name, name, enabled, subobjects, dynamic, mappers);
        this.containsDimensions = containsDimensions;
    }

    @Override
    public PassthroughObjectMapper.Builder newBuilder(IndexVersion indexVersionCreated) {
        PassthroughObjectMapper.Builder builder = new PassthroughObjectMapper.Builder(name());
        builder.enabled = enabled;
        builder.dynamic = dynamic;
        return builder;
    }

    @Override
    public PassthroughObjectMapper merge(Mapper mergeWith, MergeReason reason, MapperBuilderContext parentBuilderContext) {
        final var mergeResult = MergeResult.build(this, mergeWith, reason, parentBuilderContext);
        PassthroughObjectMapper mergeWithObject = (PassthroughObjectMapper) mergeWith;

        final Explicit<Boolean> containsDimensions = (mergeWithObject.containsDimensions.explicit())
            ? mergeWithObject.containsDimensions
            : this.containsDimensions;

        return new PassthroughObjectMapper(
            simpleName(),
            mergeResult.enabled(),
            mergeResult.subObjects(),
            mergeResult.dynamic(),
            mergeResult.mappers(),
            containsDimensions
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());
        builder.field("type", CONTENT_TYPE);
        if (containsDimensions.explicit()) {
            builder.field(TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM, containsDimensions.value());
        }
        if (dynamic != null) {
            builder.field("dynamic", dynamic.name().toLowerCase(Locale.ROOT));
        }
        if (isEnabled() != Defaults.ENABLED) {
            builder.field("enabled", enabled.value());
        }
        serializeMappers(builder, params);
        return builder.endObject();
    }

    public static class TypeParser extends ObjectMapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext)
            throws MapperParsingException {
            PassthroughObjectMapper.Builder builder = new Builder(name);

            parsePassthrough(name, node, builder);
            parseObjectFields(node, parserContext, builder);
            return builder;
        }

        protected static void parsePassthrough(String name, Map<String, Object> node, PassthroughObjectMapper.Builder builder) {
            Object fieldNode = node.get(TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM);
            if (fieldNode != null) {
                builder.containsDimensions = Explicit.explicitBoolean(
                    nodeBooleanValue(fieldNode, name + TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM)
                );
                node.remove(TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM);
            }
        }
    }
}
