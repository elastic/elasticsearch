/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.parser;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.StatusHeuristic;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.xpack.inference.common.parser.DateParser;
import org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.metadata.EndpointMetadata.DENIED_BY_REGION_POLICY_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.DISPLAY_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Display.MODEL_CREATOR_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Display.NAME_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.EndpointRegion.CSP_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.EndpointRegion.GEO_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.EndpointRegion.REGION_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.HEURISTICS_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Heuristics.END_OF_LIFE_DATE_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Heuristics.PROPERTIES_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Heuristics.RELEASE_DATE_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Heuristics.STATUS_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.INTERNAL_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Internal.FINGERPRINT_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Internal.VERSION_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.METADATA_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.REGIONS_FIELD_NAME;
import static org.elasticsearch.xpack.inference.common.parser.EnumParser.extractEnum;
import static org.elasticsearch.xpack.inference.common.parser.NumberParser.extractLong;
import static org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils.isMapNullOrEmpty;
import static org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils.pathToKey;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.extractStringList;

/**
 * Parser for {@link EndpointMetadata} and its nested types from a {@link Map}&lt;String, Object&gt;
 * that has the same structure as the JSON produced by {@link EndpointMetadata#toXContent}.
 */
public final class EndpointMetadataParser {

    /**
     * Parse {@link EndpointMetadata} from a map with the same structure as the JSON produced by
     * {@link EndpointMetadata#toXContent}.
     *
     * @param map The map to parse from
     * @return the {@link EndpointMetadata}. {@link EndpointMetadata#EMPTY_INSTANCE} is returned if the map is null or empty
     */
    public static EndpointMetadata fromMap(@Nullable Map<String, Object> map) {
        if (isMapNullOrEmpty(map)) {
            return EndpointMetadata.EMPTY_INSTANCE;
        }

        var metadataMap = ServiceUtils.removeFromMap(map, METADATA_FIELD_NAME);
        if (isMapNullOrEmpty(metadataMap)) {
            return EndpointMetadata.EMPTY_INSTANCE;
        }

        var heuristicsMap = ServiceUtils.removeFromMap(metadataMap, HEURISTICS_FIELD_NAME);
        var internalMap = ServiceUtils.removeFromMap(metadataMap, INTERNAL_FIELD_NAME);
        var displayMap = ServiceUtils.removeFromMap(metadataMap, DISPLAY_FIELD_NAME);

        var heuristics = heuristicsFromMap(heuristicsMap, pathToKey(METADATA_FIELD_NAME, HEURISTICS_FIELD_NAME));
        var internal = internalFromMap(internalMap, pathToKey(METADATA_FIELD_NAME, INTERNAL_FIELD_NAME));
        var display = displayFromMap(displayMap, pathToKey(METADATA_FIELD_NAME, DISPLAY_FIELD_NAME));
        var regions = regionsFromMap(metadataMap, pathToKey(METADATA_FIELD_NAME, REGIONS_FIELD_NAME));
        var deniedByRegionPolicy = deniedByRegionPolicyFromMap(
            metadataMap,
            pathToKey(METADATA_FIELD_NAME, DENIED_BY_REGION_POLICY_FIELD_NAME)
        );

        var endpointMetadata = new EndpointMetadata(heuristics, internal, display, regions, deniedByRegionPolicy);
        return EndpointMetadata.EMPTY_INSTANCE.equals(endpointMetadata) ? EndpointMetadata.EMPTY_INSTANCE : endpointMetadata;
    }

    /**
     * Parse {@link EndpointMetadata.Heuristics} from a map with the same structure as the JSON produced by
     * {@link EndpointMetadata.Heuristics#toXContent}.
     * Returns {@link EndpointMetadata.Heuristics#EMPTY_INSTANCE} if the map is null or empty.
     */
    static EndpointMetadata.Heuristics heuristicsFromMap(@Nullable Map<String, Object> map, String root) {
        if (map == null || map.isEmpty()) {
            return EndpointMetadata.Heuristics.EMPTY_INSTANCE;
        }
        var properties = extractStringList(map, PROPERTIES_FIELD_NAME, root, List.of());
        var status = extractEnum(map, STATUS_FIELD_NAME, root, StatusHeuristic::fromString, EnumSet.allOf(StatusHeuristic.class));
        var releaseDate = DateParser.parseLocalDate(map, RELEASE_DATE_FIELD_NAME, root);
        var endOfLifeDate = DateParser.parseLocalDate(map, END_OF_LIFE_DATE_FIELD_NAME, root);

        if (properties.isEmpty() && status == null && releaseDate == null && endOfLifeDate == null) {
            return EndpointMetadata.Heuristics.EMPTY_INSTANCE;
        }

        return new EndpointMetadata.Heuristics(properties, status, releaseDate, endOfLifeDate);
    }

    /**
     * Parse {@link EndpointMetadata.Internal} from a map with the same structure as the JSON produced by
     * {@link EndpointMetadata.Internal#toXContent}. Returns {@link EndpointMetadata.Internal#EMPTY_INSTANCE} if the map is null or empty.
     */
    static EndpointMetadata.Internal internalFromMap(@Nullable Map<String, Object> map, String root) {
        if (map == null || map.isEmpty()) {
            return EndpointMetadata.Internal.EMPTY_INSTANCE;
        }
        var fingerprint = ObjectParserUtils.removeAsType(map, FINGERPRINT_FIELD_NAME, root, String.class);
        var version = extractLong(map, VERSION_FIELD_NAME, root);

        if (fingerprint == null && version == null) {
            return EndpointMetadata.Internal.EMPTY_INSTANCE;
        }

        return new EndpointMetadata.Internal(fingerprint, version);
    }

    /**
     * Parse {@link EndpointMetadata.Display} from a map with the same structure as the JSON produced by
     * {@link EndpointMetadata.Display#toXContent}. Returns null if the map is null or empty.
     */
    static EndpointMetadata.Display displayFromMap(@Nullable Map<String, Object> map, String root) {
        if (map == null || map.isEmpty()) {
            return EndpointMetadata.Display.EMPTY_INSTANCE;
        }
        var name = ObjectParserUtils.removeAsType(map, NAME_FIELD, root, String.class);
        var modelCreator = ObjectParserUtils.removeAsType(map, MODEL_CREATOR_FIELD, root, String.class);

        if (name == null && modelCreator == null) {
            return EndpointMetadata.Display.EMPTY_INSTANCE;
        }

        return new EndpointMetadata.Display(name, modelCreator);
    }

    /**
     * Parse a list of {@link EndpointMetadata.EndpointRegion} from the parent metadata map.
     * Returns an empty list if the field is absent or the list is empty.
     */
    @SuppressWarnings("unchecked")
    static List<EndpointMetadata.EndpointRegion> regionsFromMap(@Nullable Map<String, Object> map, String root) {
        if (isMapNullOrEmpty(map)) {
            return List.of();
        }
        var rawList = ObjectParserUtils.removeAsType(map, REGIONS_FIELD_NAME, root, List.class);
        if (rawList == null || rawList.isEmpty()) {
            return List.of();
        }
        var regions = new ArrayList<EndpointMetadata.EndpointRegion>(rawList.size());
        for (int i = 0; i < rawList.size(); i++) {
            var item = rawList.get(i);
            if (item instanceof Map == false) {
                throw new IllegalArgumentException(
                    ObjectParserUtils.invalidTypeErrorMsg(REGIONS_FIELD_NAME + "[" + i + "]", root, item, "Map")
                );
            }
            var regionMap = (Map<String, Object>) item;
            var regionRoot = root + "[" + i + "]";
            var csp = ObjectParserUtils.removeAsType(regionMap, CSP_FIELD.getPreferredName(), regionRoot, String.class);
            var region = ObjectParserUtils.removeAsType(regionMap, REGION_FIELD.getPreferredName(), regionRoot, String.class);
            var geo = ObjectParserUtils.removeAsType(regionMap, GEO_FIELD.getPreferredName(), regionRoot, String.class);
            regions.add(new EndpointMetadata.EndpointRegion(csp, region, geo));
        }
        return List.copyOf(regions);
    }

    /**
     * Parse the {@code denied_by_region_policy} boolean flag from the parent metadata map.
     * Returns {@code false} if the field is absent.
     */
    static boolean deniedByRegionPolicyFromMap(@Nullable Map<String, Object> map, String root) {
        if (isMapNullOrEmpty(map)) {
            return false;
        }
        var value = ObjectParserUtils.removeAsType(map, DENIED_BY_REGION_POLICY_FIELD_NAME, root, Boolean.class);
        return value == null ? false : value;
    }

    private EndpointMetadataParser() {}
}
