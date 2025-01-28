/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Explicit;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;

/**
 * Mapper for pass-through objects.
 *
 * Pass-through objects allow creating fields inside them that can also be referenced directly in search queries.
 * They also include parameters that affect how nested fields get configured. For instance, if parameter "time_series_dimension"
 * is set, eligible subfields are marked as dimensions and keyword fields are additionally included in routing and tsid calculations.
 *
 * In case different pass-through objects contain subfields with the same name (excluding the pass-through prefix), their aliases conflict.
 * To resolve this, the pass-through spec specifies which object takes precedence through required parameter "priority"; non-negative
 * integer values are accepted, with the highest priority value winning in case of conflicting aliases.
 */
public class PassThroughObjectMapper extends ObjectMapper {
    public static final String CONTENT_TYPE = "passthrough";
    public static final String PRIORITY_PARAM_NAME = "priority";

    public static class Builder extends ObjectMapper.Builder {

        // Controls whether subfields are configured as time-series dimensions.
        protected Explicit<Boolean> timeSeriesDimensionSubFields = Explicit.IMPLICIT_FALSE;

        // Controls which pass-through fields take precedence in case of conflicting aliases.
        protected int priority = -1;

        public Builder(String name) {
            // Subobjects are not currently supported.
            super(name, Optional.of(Subobjects.DISABLED));
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

        public PassThroughObjectMapper.Builder setPriority(int priority) {
            this.priority = priority;
            return this;
        }

        @Override
        public PassThroughObjectMapper build(MapperBuilderContext context) {
            return new PassThroughObjectMapper(
                leafName(),
                context.buildFullName(leafName()),
                enabled,
                sourceKeepMode,
                dynamic,
                buildMappers(context.createChildContext(leafName(), timeSeriesDimensionSubFields.value(), dynamic)),
                timeSeriesDimensionSubFields,
                priority
            );
        }
    }

    // If set, its subfields are marked as time-series dimensions (for the types supporting this).
    private final Explicit<Boolean> timeSeriesDimensionSubFields;

    private final int priority;

    PassThroughObjectMapper(
        String name,
        String fullPath,
        Explicit<Boolean> enabled,
        Optional<SourceKeepMode> sourceKeepMode,
        Dynamic dynamic,
        Map<String, Mapper> mappers,
        Explicit<Boolean> timeSeriesDimensionSubFields,
        int priority
    ) {
        // Subobjects are not currently supported.
        super(name, fullPath, enabled, Optional.of(Subobjects.DISABLED), sourceKeepMode, dynamic, mappers);
        this.timeSeriesDimensionSubFields = timeSeriesDimensionSubFields;
        this.priority = priority;
        if (priority < 0) {
            throw new MapperException("Pass-through object [" + fullPath + "] is missing a non-negative value for parameter [priority]");
        }
    }

    @Override
    PassThroughObjectMapper withoutMappers() {
        return new PassThroughObjectMapper(
            leafName(),
            fullPath(),
            enabled,
            sourceKeepMode,
            dynamic,
            Map.of(),
            timeSeriesDimensionSubFields,
            priority
        );
    }

    @Override
    public String typeName() {
        return CONTENT_TYPE;
    }

    public boolean containsDimensions() {
        return timeSeriesDimensionSubFields.value();
    }

    public int priority() {
        return priority;
    }

    @Override
    public PassThroughObjectMapper.Builder newBuilder(IndexVersion indexVersionCreated) {
        PassThroughObjectMapper.Builder builder = new PassThroughObjectMapper.Builder(leafName());
        builder.enabled = enabled;
        builder.dynamic = dynamic;
        builder.timeSeriesDimensionSubFields = timeSeriesDimensionSubFields;
        builder.priority = priority;
        return builder;
    }

    @Override
    public PassThroughObjectMapper merge(Mapper mergeWith, MapperMergeContext parentBuilderContext) {
        if (mergeWith instanceof PassThroughObjectMapper == false) {
            MapperErrors.throwObjectMappingConflictError(mergeWith.fullPath());
        }

        PassThroughObjectMapper mergeWithObject = (PassThroughObjectMapper) mergeWith;
        final var mergeResult = MergeResult.build(this, mergeWithObject, parentBuilderContext);

        final Explicit<Boolean> containsDimensions = (mergeWithObject.timeSeriesDimensionSubFields.explicit())
            ? mergeWithObject.timeSeriesDimensionSubFields
            : this.timeSeriesDimensionSubFields;

        return new PassThroughObjectMapper(
            leafName(),
            fullPath(),
            mergeResult.enabled(),
            mergeResult.sourceKeepMode(),
            mergeResult.dynamic(),
            mergeResult.mappers(),
            containsDimensions,
            Math.max(priority, mergeWithObject.priority)
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(leafName());
        builder.field("type", CONTENT_TYPE);
        if (timeSeriesDimensionSubFields.explicit()) {
            builder.field(TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM, timeSeriesDimensionSubFields.value());
        }
        if (priority >= 0) {
            builder.field(PRIORITY_PARAM_NAME, priority);
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
            fieldNode = node.get(PRIORITY_PARAM_NAME);
            if (fieldNode != null) {
                builder.priority = nodeIntegerValue(fieldNode);
                node.remove(PRIORITY_PARAM_NAME);
            }
        }
    }

    /**
     * Checks the passed objects for duplicate or negative priorities.
     * @param passThroughMappers objects to check
     */
    public static void checkForDuplicatePriorities(Collection<PassThroughObjectMapper> passThroughMappers) {
        Map<Integer, String> seen = new HashMap<>();
        for (PassThroughObjectMapper mapper : passThroughMappers) {
            String conflict = seen.put(mapper.priority, mapper.fullPath());
            if (conflict != null) {
                throw new MapperException(
                    "Pass-through object ["
                        + mapper.fullPath()
                        + "] has a conflicting param [priority="
                        + mapper.priority
                        + "] with object ["
                        + conflict
                        + "]"
                );
            }
        }
    }
}
