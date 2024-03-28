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
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;

/**
 * Mapper for pass-through objects.
 *
 * Pass-through objects allow creating fields inside them that can also be referenced directly in search queries.
 * They also include parameters that affect how nested fields get configured. For instance, if parameter "time_series_dimension"
 * is set, eligible subfields are marked as dimensions and keyword fields are additionally included in routing and tsid calculations.
 *
 * In case different pass-through objects contain subfields with the same name (excluding the pass-through prefix), their aliases conflict.
 * To resolve this, the pass-through spec can specify which object takes precedence by including it in the "superseded_by" parameter.
 * Otherwise, precedence is based on lexicographical ordering.
 */
public class PassThroughObjectMapper extends ObjectMapper {
    public static final String CONTENT_TYPE = "passthrough";
    public static final String SUPERSEDED_BY_PARAM = "superseded_by";

    private static final int CIRCULAR_DEPENDENCY_MAX_OPS = 10_000;

    public static class Builder extends ObjectMapper.Builder {

        // Controls whether subfields are configured as time-series dimensions.
        protected Explicit<Boolean> timeSeriesDimensionSubFields = Explicit.IMPLICIT_FALSE;

        // Controls which pass-through fields take precedence in case of conflicting aliases.
        protected Set<String> supersededBy = Set.of();

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
                timeSeriesDimensionSubFields,
                supersededBy
            );
        }
    }

    // If set, its subfields are marked as time-series dimensions (for the types supporting this).
    private final Explicit<Boolean> timeSeriesDimensionSubFields;

    private final Set<String> supersededBy;

    PassThroughObjectMapper(
        String name,
        String fullPath,
        Explicit<Boolean> enabled,
        Dynamic dynamic,
        Map<String, Mapper> mappers,
        Explicit<Boolean> timeSeriesDimensionSubFields,
        Set<String> supersededBy
    ) {
        // Subobjects are not currently supported.
        super(name, fullPath, enabled, Explicit.IMPLICIT_FALSE, dynamic, mappers);
        this.timeSeriesDimensionSubFields = timeSeriesDimensionSubFields;
        this.supersededBy = supersededBy;

        if (supersededBy.contains(fullPath)) {
            throw new MapperException(
                "Mapping definition for [" + fullPath + "] contains a self-reference in param [" + SUPERSEDED_BY_PARAM + "]"
            );
        }
    }

    @Override
    PassThroughObjectMapper withoutMappers() {
        return new PassThroughObjectMapper(
            simpleName(),
            fullPath(),
            enabled,
            dynamic,
            Map.of(),
            timeSeriesDimensionSubFields,
            supersededBy
        );
    }

    public boolean containsDimensions() {
        return timeSeriesDimensionSubFields.value();
    }

    public Set<String> supersededBy() {
        return supersededBy;
    }

    @Override
    public PassThroughObjectMapper.Builder newBuilder(IndexVersion indexVersionCreated) {
        PassThroughObjectMapper.Builder builder = new PassThroughObjectMapper.Builder(simpleName());
        builder.enabled = enabled;
        builder.dynamic = dynamic;
        builder.timeSeriesDimensionSubFields = timeSeriesDimensionSubFields;
        builder.supersededBy = supersededBy;
        return builder;
    }

    public PassThroughObjectMapper merge(ObjectMapper mergeWith, MergeReason reason, MapperMergeContext parentBuilderContext) {
        final var mergeResult = MergeResult.build(this, mergeWith, reason, parentBuilderContext);
        PassThroughObjectMapper mergeWithObject = (PassThroughObjectMapper) mergeWith;

        final Explicit<Boolean> containsDimensions = (mergeWithObject.timeSeriesDimensionSubFields.explicit())
            ? mergeWithObject.timeSeriesDimensionSubFields
            : this.timeSeriesDimensionSubFields;

        Set<String> mergedSupersededBy = new TreeSet<>(supersededBy);
        mergedSupersededBy.addAll(mergeWithObject.supersededBy);

        return new PassThroughObjectMapper(
            simpleName(),
            fullPath(),
            mergeResult.enabled(),
            mergeResult.dynamic(),
            mergeResult.mappers(),
            containsDimensions,
            mergedSupersededBy
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());
        builder.field("type", CONTENT_TYPE);
        if (timeSeriesDimensionSubFields.explicit()) {
            builder.field(TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM, timeSeriesDimensionSubFields.value());
        }
        if (supersededBy.isEmpty() == false) {
            builder.field(SUPERSEDED_BY_PARAM, supersededBy);
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
            fieldNode = node.get(SUPERSEDED_BY_PARAM);
            if (fieldNode != null) {
                builder.supersededBy = new TreeSet<>(Arrays.asList(nodeStringArrayValue(fieldNode)));
                node.remove(SUPERSEDED_BY_PARAM);
            }
        }
    }

    /**
     * Checks the passed objects for circular dependencies on the superseded_by cross-dependencies.
     * @param passThroughMappers objects to check
     * @return A list containing the circular dependency chain, or an empty list if no circular dependency is found
     */
    public static List<PassThroughObjectMapper> checkSupersedesForCircularDeps(Collection<PassThroughObjectMapper> passThroughMappers) {
        if (passThroughMappers.size() < 2) {
            return List.of();
        }

        // Perform DFS.
        // The implementation below assumes that the graph is rather small and sparse. Further optimizations are
        // required (e.g. use sets for lookups) if production systems end up with hundreds of tightly-coupled objects.
        int i = 0;
        Map<String, PassThroughObjectMapper> lookupByName = passThroughMappers.stream()
            .collect(Collectors.toMap(PassThroughObjectMapper::name, Function.identity()));
        for (PassThroughObjectMapper start : passThroughMappers) {
            Stack<PassThroughObjectMapper> sequence = new Stack<>();
            Map<PassThroughObjectMapper, PassThroughObjectMapper> reverseDeps = new HashMap<>();
            Stack<PassThroughObjectMapper> pending = new Stack<>();
            pending.push(start);
            while (pending.empty() == false) {
                if (++i > CIRCULAR_DEPENDENCY_MAX_OPS) {  // Defensive check.
                    break;
                }
                PassThroughObjectMapper current = pending.pop();
                // If we reached the end of a branch, unwind the stack till the beginning of the next branch.
                while (sequence.isEmpty() == false && reverseDeps.get(current) != sequence.peek()) {
                    sequence.pop();
                }
                // Check if the branch contains the same element twice.
                if (sequence.contains(current)) {
                    sequence.push(current);
                    return sequence;
                }
                sequence.push(current);
                for (String dep : current.supersededBy()) {
                    PassThroughObjectMapper mapper = lookupByName.get(dep);
                    if (mapper != null) {
                        pending.push(mapper);
                        reverseDeps.put(mapper, current);
                    }
                }
            }
        }
        return List.of();
    }
}
