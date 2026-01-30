/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.parser;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EndpointMetadata;
import org.elasticsearch.inference.StatusHeuristic;
import org.elasticsearch.xpack.inference.common.parser.DateParser;
import org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.util.EnumSet;
import java.util.Map;

import static org.elasticsearch.inference.EndpointMetadata.HEURISTICS;
import static org.elasticsearch.inference.EndpointMetadata.Heuristics.END_OF_LIFE_DATE;
import static org.elasticsearch.inference.EndpointMetadata.Heuristics.PROPERTIES;
import static org.elasticsearch.inference.EndpointMetadata.Heuristics.RELEASE_DATE;
import static org.elasticsearch.inference.EndpointMetadata.Heuristics.STATUS;
import static org.elasticsearch.inference.EndpointMetadata.INTERNAL;
import static org.elasticsearch.inference.EndpointMetadata.Internal.FINGERPRINT;
import static org.elasticsearch.inference.EndpointMetadata.Internal.VERSION;
import static org.elasticsearch.inference.EndpointMetadata.METADATA;
import static org.elasticsearch.inference.EndpointMetadata.NAME;
import static org.elasticsearch.xpack.inference.common.parser.EnumParser.extractEnum;
import static org.elasticsearch.xpack.inference.common.parser.NumberParser.extractNumber;
import static org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils.isMapNullOrEmpty;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.extractStringList;

/**
 * Parser for {@link EndpointMetadata} and its nested types from a {@link Map}&lt;String, Object&gt;
 * that has the same structure as the JSON produced by {@link EndpointMetadata#toXContent}.
 */
public final class EndpointMetadataParser {

    public static void discardEndpointMetadata(Map<String, Object> map) {
        ServiceUtils.removeFromMap(map, METADATA);
    }

    /**
     * Parse {@link EndpointMetadata} from a map with the same structure as the JSON produced by
     * {@link EndpointMetadata#toXContent}. Returns {@link EndpointMetadata#EMPTY} if the map is null or empty.
     */
    public static EndpointMetadata fromMap(@Nullable Map<String, Object> map, String root) {
        if (isMapNullOrEmpty(map)) {
            return EndpointMetadata.EMPTY;
        }

        var metadataMap = ServiceUtils.removeFromMap(map, METADATA);
        if (isMapNullOrEmpty(metadataMap)) {
            return EndpointMetadata.EMPTY;
        }

        var heuristicsMap = ServiceUtils.removeFromMap(metadataMap, HEURISTICS);
        var internalMap = ServiceUtils.removeFromMap(metadataMap, INTERNAL);
        var name = ObjectParserUtils.removeAsType(metadataMap, NAME, root + "." + METADATA, String.class);

        var heuristics = heuristicsFromMap(heuristicsMap, root);
        var internal = internalFromMap(internalMap, root);
        return new EndpointMetadata(heuristics, internal, name);
    }

    /**
     * Parse {@link EndpointMetadata.Heuristics} from a map with the same structure as the JSON produced by
     * {@link EndpointMetadata.Heuristics#toXContent}. Returns {@link EndpointMetadata.Heuristics#EMPTY} if the map is null or empty.
     */
    public static EndpointMetadata.Heuristics heuristicsFromMap(@Nullable Map<String, Object> map, String root) {
        if (map == null || map.isEmpty()) {
            return EndpointMetadata.Heuristics.EMPTY;
        }
        var properties = extractStringList(map, PROPERTIES, root);
        var status = extractEnum(map, STATUS, root, StatusHeuristic::fromString, EnumSet.allOf(StatusHeuristic.class));
        var releaseDate = DateParser.parseLocalDate(map, RELEASE_DATE, root);
        var endOfLifeDate = DateParser.parseLocalDate(map, END_OF_LIFE_DATE, root);

        return new EndpointMetadata.Heuristics(properties, status, releaseDate, endOfLifeDate);
    }

    /**
     * Parse {@link EndpointMetadata.Internal} from a map with the same structure as the JSON produced by
     * {@link EndpointMetadata.Internal#toXContent}. Returns {@link EndpointMetadata.Internal#EMPTY} if the map is null or empty.
     */
    public static EndpointMetadata.Internal internalFromMap(@Nullable Map<String, Object> map, String root) {
        if (map == null || map.isEmpty()) {
            return EndpointMetadata.Internal.EMPTY;
        }
        var fingerprint = ServiceUtils.removeAsType(map, FINGERPRINT, String.class);
        var version = extractNumber(map, VERSION, root);
        return new EndpointMetadata.Internal(fingerprint, version == null ? null : version.longValue());
    }

    private EndpointMetadataParser() {}
}
