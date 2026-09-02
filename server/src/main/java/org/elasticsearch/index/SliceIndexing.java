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
import org.elasticsearch.rest.RestRequest;

import java.util.regex.Pattern;

/**
 * Centralizes slice-indexing feature gating.
 */
public final class SliceIndexing {

    private SliceIndexing() {}

    /** REST request parameter name (mirrors {@code routing}); the field/output form is {@link #FIELD_NAME}. */
    public static final String PARAM_NAME = "slice";
    /** Metadata field / script-context name (mirrors {@code _routing}); the request-parameter form is {@link #PARAM_NAME}. */
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
    private static final int MAX_SLICE_VALUE_LENGTH = 128;
    private static final Pattern VALID_SLICE_VALUE_PATTERN = Pattern.compile("[a-zA-Z0-9](?:[a-zA-Z0-9._:-]*[a-zA-Z0-9])?");

    /**
     * A reserved value for the REST-only {@code slice} search parameter meaning "do not restrict to a routing value".
     * This is used to query across all slices while still indicating intentional slice-mode access.
     */
    public static final String SLICE_ALL = "_all";

    /**
     * Parsed routing result with provenance indicating if the value came from {@code slice}.
     */
    public record ParsedRouting(String routing, boolean fromSlice) {}

    /**
     * Validates user-supplied {@code slice} values accepted by REST write APIs.
     */
    public static void validateUserSliceValue(String slice) {
        if (slice.isEmpty()) {
            throw new IllegalArgumentException("invalid [slice] value: value must be non-empty");
        }
        if (slice.length() > MAX_SLICE_VALUE_LENGTH) {
            throw new IllegalArgumentException(
                "invalid [slice] value [" + slice + "]: length [" + slice.length() + "] exceeds max [" + MAX_SLICE_VALUE_LENGTH + "]"
            );
        }
        if (SLICE_ALL.equals(slice)) {
            throw new IllegalArgumentException("invalid [slice] value [" + slice + "]: value is reserved");
        }
        if (VALID_SLICE_VALUE_PATTERN.matcher(slice).matches() == false) {
            throw new IllegalArgumentException(
                "invalid [slice] value [" + slice + "]: only [a-zA-Z0-9._:-] are allowed and max length is [" + MAX_SLICE_VALUE_LENGTH + "]"
            );
        }
    }

    /**
     * Parses and validates the REST-level {@code routing} and {@code slice} parameters.
     * Returns the effective routing value and whether it was provided via {@code slice}.
     */
    public static ParsedRouting parseRoutingOrSliceWithProvenance(RestRequest request) {
        final String routing = request.param("routing");
        final String slice = request.param(PARAM_NAME);
        if (slice != null && SLICE_FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalArgumentException("request does not support [slice]");
        }
        if (slice != null) {
            validateUserSliceValue(slice);
        }
        if (slice != null && routing != null) {
            throw new IllegalArgumentException("[routing] is not allowed together with [slice]");
        }
        return new ParsedRouting(slice != null ? slice : routing, slice != null);
    }

    /**
     * Parses and validates the REST-level {@code routing} and {@code slice} parameters for search APIs.
     * If {@code slice} is supplied, the returned routing contains the effective routing values
     * (or {@code null} for {@code slice=_all}).
     */
    public static ParsedRouting parseSearchRoutingOrSliceWithProvenance(RestRequest request) {
        final String routing = request.param("routing");
        final String slice = request.param(PARAM_NAME);
        if (slice != null && SLICE_FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalArgumentException("request does not support [slice]");
        }
        if (slice != null && routing != null) {
            throw new IllegalArgumentException("[routing] is not allowed together with [slice]");
        }
        if (slice == null) {
            return new ParsedRouting(routing, false);
        }
        if (SLICE_ALL.equals(slice)) {
            return new ParsedRouting(null, true);
        }
        final String[] slices = Strings.splitStringByCommaToArray(slice);
        if (slices.length == 0) {
            throw new IllegalArgumentException("invalid [slice] value: value must be non-empty");
        }
        for (String sliceValue : slices) {
            validateUserSliceValue(sliceValue);
        }
        return new ParsedRouting(String.join(",", slices), true);
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
                "[slice] is not allowed when [index.slice.enabled] is false for " + requestDescription + " targeting [" + target + "]"
            );
        }
        if (sliceEnabled && routingFromSlice == false) {
            if (routing != null) {
                throw new IllegalArgumentException(
                    "[routing] is not allowed when [index.slice.enabled] is true for "
                        + requestDescription
                        + " targeting ["
                        + target
                        + "], use [slice] instead"
                );
            }
            throw new IllegalArgumentException(
                "[slice] is required when [index.slice.enabled] is true for " + requestDescription + " targeting [" + target + "]"
            );
        }
    }

    /**
     * Validates request-level slice/routing requirements and resolves effective routing for search-style APIs.
     * When {@code anySliceEnabled} is true and no {@code slice} parameter was provided, the request is treated
     * as {@code slice=_all} (routing is left unrestricted, covering all slices).
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
                    + "], use [slice] instead"
            );
        }
        if (routingFromSlice && anySliceEnabled == false && allowSliceWhenNoLocalSliceEnabled == false) {
            throw new IllegalArgumentException(
                "[slice] is not allowed when [index.slice.enabled] is false for " + requestDescription + " targeting [" + target + "]"
            );
        }
        if (routingFromSlice) {
            return SLICE_ALL.equals(requestedSlice) ? null : requestedSlice;
        }
        return routing;
    }

}
