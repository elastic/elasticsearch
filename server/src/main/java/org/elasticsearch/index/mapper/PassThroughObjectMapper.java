/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Explicit;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Mapper for pass-through objects.
 *
 * Pass-through objects allow creating fields inside them that can also be referenced directly in search queries.
 * They also include parameters that affect how nested fields get configured. For instance, if parameter "time_series_dimension"
 * is set, eligible subfields are marked as dimensions and keyword fields are additionally included in routing and tsid calculations.
 */
public class PassThroughObjectMapper extends ObjectMapper {
    public static final String CONTENT_TYPE = "passthrough";

    public static class Builder extends ObjectMapper.Builder {

        // Controls whether subfields are configured as time-series dimensions.
        protected Explicit<Boolean> timeSeriesDimensionSubFields = Explicit.IMPLICIT_FALSE;

        public Builder(String name) {
            // Subobjects are not currently supported.
            super(name, Explicit.IMPLICIT_FALSE);
        }

        @Override
        public PassThroughObjectMapper.Builder add(Mapper.Builder builder) {
            if (timeSeriesDimensionSubFields.value() && builder instanceof FieldMapper.DimensionBuilder dimensionBuilder) {
                dimensionBuilder.setInheritDimensionParameterFromParentObject();
            }
            super.add(builder);
            return this;
        }

        public PassThroughObjectMapper.Builder setContainsDimensions() {
            timeSeriesDimensionSubFields = Explicit.EXPLICIT_TRUE;
            return this;
        }

        @Override
        public PassThroughObjectMapper build(MapperBuilderContext context) {
            return new PassThroughObjectMapper(
                name(),
                context.buildFullName(name()),
                enabled,
                dynamic,
                buildMappers(context.createChildContext(name(), timeSeriesDimensionSubFields.value(), dynamic)),
                timeSeriesDimensionSubFields
            );
        }
    }

    // If set, its subfields are marked as time-series dimensions (for the types supporting this).
    private final Explicit<Boolean> timeSeriesDimensionSubFields;

    PassThroughObjectMapper(
        String name,
        String fullPath,
        Explicit<Boolean> enabled,
        Dynamic dynamic,
        Map<String, Mapper> mappers,
        Explicit<Boolean> timeSeriesDimensionSubFields
    ) {
        // Subobjects are not currently supported.
        super(name, fullPath, enabled, Explicit.IMPLICIT_FALSE, dynamic, mappers);
        this.timeSeriesDimensionSubFields = timeSeriesDimensionSubFields;
    }

    @Override
    PassThroughObjectMapper withoutMappers() {
        return new PassThroughObjectMapper(simpleName(), fullPath(), enabled, dynamic, Map.of(), timeSeriesDimensionSubFields);
    }

    public boolean containsDimensions() {
        return timeSeriesDimensionSubFields.value();
    }

    @Override
    public PassThroughObjectMapper.Builder newBuilder(IndexVersion indexVersionCreated) {
        PassThroughObjectMapper.Builder builder = new PassThroughObjectMapper.Builder(simpleName());
        builder.enabled = enabled;
        builder.dynamic = dynamic;
        builder.timeSeriesDimensionSubFields = timeSeriesDimensionSubFields;
        return builder;
    }

    @Override
    public PassThroughObjectMapper merge(Mapper mergeWith, MapperMergeContext parentBuilderContext) {
        if (mergeWith instanceof PassThroughObjectMapper == false) {
            MapperErrors.throwObjectMappingConflictError(mergeWith.name());
        }

        PassThroughObjectMapper mergeWithObject = (PassThroughObjectMapper) mergeWith;
        final var mergeResult = MergeResult.build(this, mergeWithObject, parentBuilderContext);

        final Explicit<Boolean> containsDimensions = (mergeWithObject.timeSeriesDimensionSubFields.explicit())
            ? mergeWithObject.timeSeriesDimensionSubFields
            : this.timeSeriesDimensionSubFields;

        return new PassThroughObjectMapper(
            simpleName(),
            fullPath(),
            mergeResult.enabled(),
            mergeResult.dynamic(),
            mergeResult.mappers(),
            containsDimensions
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());
        builder.field("type", CONTENT_TYPE);
        if (timeSeriesDimensionSubFields.explicit()) {
            builder.field(TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM, timeSeriesDimensionSubFields.value());
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
            PassThroughObjectMapper.Builder builder = new Builder(name);
            parsePassthrough(name, node, builder);
            parseObjectFields(node, parserContext, builder);
            return builder;
        }

        protected static void parsePassthrough(String name, Map<String, Object> node, PassThroughObjectMapper.Builder builder) {
            Object fieldNode = node.get(TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM);
            if (fieldNode != null) {
                builder.timeSeriesDimensionSubFields = Explicit.explicitBoolean(
                    nodeBooleanValue(fieldNode, name + TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM)
                );
                node.remove(TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM);
            }
        }
    }
}
