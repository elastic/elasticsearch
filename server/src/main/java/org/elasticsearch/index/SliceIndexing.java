/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.rest.RequestParams;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestRequest;

import java.util.regex.Pattern;

/**
 * Centralizes slice-indexing feature gating.
 */
public final class SliceIndexing {

    private SliceIndexing() {}

    /** Slice identifier name used everywhere: request-side (path segment, per-item body field, msearch header) and as the document metadata field / script-context name. */
    public static final String FIELD_NAME = "_slice";
    public static final FeatureFlag SLICE_FEATURE_FLAG = new FeatureFlag("slice_indexing");
    public static final TransportVersion SLICE_MISSING_EXCEPTION_VERSION = TransportVersion.fromName("slice_missing_exception");
    public static final TransportVersion REINDEX_DEST_ROUTING_PROVENANCE_VERSION = TransportVersion.fromName(
        "reindex_dest_routing_provenance"
    );
    public static final TransportVersion SEARCH_SLICE_ROUTING_STATE_VERSION = TransportVersion.fromName("search_slice_routing_state");
    public static final TransportVersion CLUSTER_SEARCH_SHARDS_SLICE_ROUTING_STATE_VERSION = TransportVersion.fromName(
        "cluster_search_shards_slice_routing_state"
    );
    public static final TransportVersion VALIDATE_QUERY_SLICE_ROUTING_STATE_VERSION = TransportVersion.fromName(
        "validate_query_slice_routing_state"
    );
    public static final TransportVersion OPEN_POINT_IN_TIME_SLICE_ROUTING_STATE_VERSION = TransportVersion.fromName(
        "open_point_in_time_slice_routing_state"
    );
    /**
     * From this version search-style requests no longer send the slice value; it is derived from routing and its provenance (also known
     * by isRoutingFromSlice).
     */
    public static final TransportVersion SLICE_ROUTING_STATE_DERIVED_VERSION = TransportVersion.fromName("slice_routing_state_derived");
    private static final int MAX_SLICE_VALUE_LENGTH = 128;
    private static final Pattern VALID_SLICE_VALUE_PATTERN = Pattern.compile("[a-zA-Z0-9](?:[a-zA-Z0-9._:-]*[a-zA-Z0-9])?");

    /**
     * A reserved value for the REST-only {@code _slice} search parameter meaning "do not restrict to a routing value".
     * This is used to query across all slices while still indicating intentional slice-mode access.
     */
    public static final String SLICE_ALL = "_all";

    /**
     * Parsed routing result with provenance indicating if the value came from {@code slice}.
     */
    public record ParsedRouting(@Nullable String routing, boolean fromSlice) {}

    /**
     * Returns the {@code slice} value implied by a routing value and its provenance: {@code null} when routing did not come from
     * {@code slice}, {@link #SLICE_ALL} when it did but is unrestricted, otherwise the routing value itself.
     */
    @Nullable
    public static String toSearchSlice(@Nullable String routing, boolean routingFromSlice) {
        if (routingFromSlice == false) {
            return null;
        }
        return routing == null ? SLICE_ALL : routing;
    }

    /**
     * Inverse of {@link #toSearchSlice}: returns the routing value implied by a {@code slice} value, where {@link #SLICE_ALL}
     * means unrestricted ({@code null}) routing.
     */
    @Nullable
    public static String sliceToRouting(String slice) {
        return SLICE_ALL.equals(slice) ? null : slice;
    }

    /**
     * Validates user-supplied {@code slice} values accepted by REST write APIs.
     */
    public static void validateUserSliceValue(String slice) {
        if (slice.isEmpty()) {
            throw new IllegalArgumentException("invalid [" + FIELD_NAME + "] value: value must be non-empty");
        }
        if (slice.length() > MAX_SLICE_VALUE_LENGTH) {
            throw new IllegalArgumentException(
                "invalid [" + FIELD_NAME + "] value [" + slice + "]: length [" + slice.length() + "] exceeds max [" + MAX_SLICE_VALUE_LENGTH + "]"
            );
        }
        if (SLICE_ALL.equals(slice)) {
            throw new IllegalArgumentException("invalid [" + FIELD_NAME + "] value [" + slice + "]: value is reserved");
        }
        if (VALID_SLICE_VALUE_PATTERN.matcher(slice).matches() == false) {
            throw new IllegalArgumentException(
                "invalid [" + FIELD_NAME + "] value [" + slice + "]: only [a-zA-Z0-9._:-] are allowed and max length is [" + MAX_SLICE_VALUE_LENGTH + "]"
            );
        }
    }

    /**
     * Parses and validates the REST-level {@code routing} and {@code _slice} for by-id document APIs (index, create, get, delete, update,
     * term vectors, mget, bulk, explain).
     * <p>
     * Slice is encoded as a path segment, for example {@code /{index}/{_slice}/_doc/{id}}; the {@code _slice} query parameter is not
     * supported. The value must be a single slice (the reserved token {@code _all} and comma-separated lists are rejected by
     * {@link #validateUserSliceValue}). Returns the effective routing value and whether it was provided via {@code _slice}.
     */
    public static ParsedRouting parseRoutingOrSliceWithProvenance(RestRequest request) {
        final String routing = request.param("routing");
        if (queryParam(request, FIELD_NAME) != null) {
            throw new IllegalArgumentException(
                "[" + FIELD_NAME + "] query parameter is not supported; use the /{index}/{_slice}/... path form"
            );
        }
        final String slice = request.param(FIELD_NAME);
        if (slice != null && SLICE_FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalArgumentException("request does not support [" + FIELD_NAME + "]");
        }
        if (slice != null) {
            validateUserSliceValue(slice);
        }
        if (slice != null && routing != null) {
            throw new IllegalArgumentException("[routing] is not allowed together with [" + FIELD_NAME + "]");
        }
        return new ParsedRouting(slice != null ? slice : routing, slice != null);
    }

    /**
     * Parses and validates slice routing for search APIs.
     * <p>
     * Slice is encoded in the path as {@code /{index}/{_slice}/_search}. The {@code {_slice}} segment may be a single slice,
     * a comma-separated list (for example {@code tenant-a,tenant-b}), or the reserved token {@code _all}.
     * The {@code _slice} query parameter is not supported for search.
     */
    public static ParsedRouting parseSearchRoutingOrSliceWithProvenance(RestRequest request) {
        final String routing = request.param("routing");
        if (queryParam(request, FIELD_NAME) != null) {
            throw new IllegalArgumentException(
                "[" + FIELD_NAME + "] query parameter is not supported for search; use /{index}/{_slice}/_search"
            );
        }
        if (isPathBasedSliceSearch(request)) {
            if (SLICE_FEATURE_FLAG.isEnabled() == false) {
                throw new IllegalArgumentException("request does not support [" + FIELD_NAME + "]");
            }
            if (routing != null) {
                throw new IllegalArgumentException("[routing] is not allowed together with [" + FIELD_NAME + "]");
            }
            final String index = request.param("index");
            if (index != null && index.contains(",")) {
                throw new IllegalArgumentException("path slice search supports a single index only");
            }
            final String pathSlice = request.param(FIELD_NAME);
            assert pathSlice != null : "path slice search must capture [" + FIELD_NAME + "] from the path";
            return parseSearchPathSlice(pathSlice);
        }
        if (routing != null) {
            return new ParsedRouting(routing, false);
        }
        return new ParsedRouting(null, false);
    }

    private static ParsedRouting parseSearchPathSlice(String pathSlice) {
        if (SLICE_ALL.equals(pathSlice)) {
            return new ParsedRouting(null, true);
        }
        final String[] slices = Strings.splitStringByCommaToArray(pathSlice);
        if (slices.length == 0) {
            throw new IllegalArgumentException("invalid [" + FIELD_NAME + "] value: value must be non-empty");
        }
        for (String sliceValue : slices) {
            validateUserSliceValue(sliceValue);
        }
        return new ParsedRouting(String.join(",", slices), true);
    }

    /**
     * Parses and validates the REST-level {@code routing} and {@code _slice} for search-family APIs that supply the slice via
     * the {@code _slice} parameter rather than a path segment: {@code _msearch}, {@code _validate/query}, {@code _search_shards},
     * and {@code _pit}.
     * <p>
     * These APIs either target multiple indices/sub-requests ({@code _msearch}) or otherwise have no per-index {@code {_slice}}
     * path segment, so the slice is supplied as a parameter (mirroring the {@code routing} parameter). The value may be a single
     * slice, a comma-separated list, or the reserved token {@code _all}. For {@code _msearch} this is the request-level default;
     * per-sub-request overrides are handled separately via the metadata line.
     */
    public static ParsedRouting parseParamRoutingOrSliceWithProvenance(RestRequest request) {
        final String routing = request.param("routing");
        final String slice = request.param(FIELD_NAME);
        if (slice != null && SLICE_FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalArgumentException("request does not support [" + FIELD_NAME + "]");
        }
        if (slice != null && routing != null) {
            throw new IllegalArgumentException("[routing] is not allowed together with [" + FIELD_NAME + "]");
        }
        if (slice == null) {
            return new ParsedRouting(routing, false);
        }
        return parseSearchPathSlice(slice);
    }

    /**
     * Returns {@code true} when the request matched a path-based slice search endpoint. The supported shapes are
     * {@code /{index}/{_slice}/_search}, {@code /{index}/{_slice}/_count}, {@code /{index}/{_slice}/_search/template}, and
     * {@code /{index}/{_slice}/_fleet/_fleet_search}.
     */
    static boolean isPathBasedSliceSearch(RestRequest request) {
        String rawPath = request.rawPath();
        final int queryStart = rawPath.indexOf('?');
        if (queryStart >= 0) {
            rawPath = rawPath.substring(0, queryStart);
        }
        final String[] parts = Strings.tokenizeToStringArray(rawPath, "/");
        if (parts.length == 3) {
            return "_search".equals(parts[2]) || "_count".equals(parts[2]);
        }
        if (parts.length == 4) {
            return ("_search".equals(parts[2]) && "template".equals(parts[3]))
                || ("_fleet".equals(parts[2]) && "_fleet_search".equals(parts[3]));
        }
        return false;
    }

    private static String queryParam(RestRequest request, String key) {
        return RequestParams.fromUri(request.uri()).get(key);
    }

    /**
     * Validates request-level slice/routing requirements for APIs that target a single index.
     */
    public static void validateSliceRoutingRequirement(
        boolean sliceEnabled,
        boolean routingFromSlice,
        String routing,
        String requestDescription,
        String target
    ) {
        if (sliceEnabled == false && routingFromSlice) {
            throw new IllegalArgumentException(
                "[" + FIELD_NAME + "] is not allowed when [index.slice.enabled] is false for " + requestDescription + " targeting [" + target + "]"
            );
        }
        if (sliceEnabled && routingFromSlice == false) {
            if (routing != null) {
                throw new IllegalArgumentException(
                    "[routing] is not allowed when [index.slice.enabled] is true for "
                        + requestDescription
                        + " targeting ["
                        + target
                        + "], use [" + FIELD_NAME + "] instead"
                );
            }
            throw new IllegalArgumentException(
                "[" + FIELD_NAME + "] is required when [index.slice.enabled] is true for " + requestDescription + " targeting [" + target + "]"
            );
        }
    }

    /**
     * Validates request-level slice/routing requirements and resolves effective routing for search-style APIs.
     * When {@code anySliceEnabled} is true and no {@code _slice} parameter was provided, the request is treated
     * as {@code _slice=_all} (routing is left unrestricted, covering all slices).
     */
    public static String validateAndResolveSliceRoutingRequirement(
        boolean anySliceEnabled,
        boolean routingFromSlice,
        String routing,
        String requestedSlice,
        String requestDescription,
        String target,
        boolean allowSliceWhenNoLocalSliceEnabled
    ) {
        if (anySliceEnabled && routingFromSlice == false && routing != null) {
            throw new IllegalArgumentException(
                "[routing] is not allowed when [index.slice.enabled] is true for "
                    + requestDescription
                    + " targeting ["
                    + target
                    + "], use [" + FIELD_NAME + "] instead"
            );
        }
        if (routingFromSlice && anySliceEnabled == false && allowSliceWhenNoLocalSliceEnabled == false) {
            throw new IllegalArgumentException(
                "[" + FIELD_NAME + "] is not allowed when [index.slice.enabled] is false for " + requestDescription + " targeting [" + target + "]"
            );
        }
        if (routingFromSlice) {
            return SLICE_ALL.equals(requestedSlice) ? null : requestedSlice;
        }
        return routing;
    }

}
