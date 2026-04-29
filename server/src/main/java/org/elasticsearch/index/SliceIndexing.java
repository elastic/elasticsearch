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
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.rest.RestRequest;

import java.util.regex.Pattern;

/**
 * Centralizes slice-indexing feature gating.
 */
public final class SliceIndexing {

    private SliceIndexing() {}

    public static final String PARAM_NAME = "_slice";
    public static final FeatureFlag SLICE_FEATURE_FLAG = new FeatureFlag("slice_indexing");
    public static final TransportVersion SLICE_MISSING_EXCEPTION_VERSION = TransportVersion.fromName("slice_missing_exception");
    private static final int MAX_SLICE_VALUE_LENGTH = 128;
    private static final Pattern VALID_SLICE_VALUE_PATTERN = Pattern.compile("[a-zA-Z0-9](?:[a-zA-Z0-9._:-]*[a-zA-Z0-9])?");

    /**
     * A reserved value for the REST-only {@code _slice} search parameter meaning "do not restrict to a routing value".
     * This is used to query across all slices while still indicating intentional slice-mode access.
     */
    public static final String SLICE_ALL = "_all";

    /**
     * Internal marker used to carry {@link #SLICE_ALL} through {@link org.elasticsearch.action.search.SearchRequest#routing(String)}
     * so the coordinator can satisfy "slice required" validation and then clear routing before shard resolution.
     *
     * This value is not realistically sendable as an HTTP query param.
     */
    public static final String SLICE_ALL_ROUTING_MARKER = "\u0000_slice_all";

    /**
     * Parsed routing result with provenance indicating if the value came from {@code _slice}.
     */
    public record ParsedRouting(String routing, boolean fromSlice) {}

    /**
     * Validates user-supplied {@code _slice} values accepted by REST write APIs.
     */
    public static void validateUserSliceValue(String slice) {
        if (slice.isEmpty()) {
            throw new IllegalArgumentException("invalid [_slice] value: value must be non-empty");
        }
        if (slice.length() > MAX_SLICE_VALUE_LENGTH) {
            throw new IllegalArgumentException(
                "invalid [_slice] value [" + slice + "]: length [" + slice.length() + "] exceeds max [" + MAX_SLICE_VALUE_LENGTH + "]"
            );
        }
        if (SLICE_ALL.equals(slice) || SLICE_ALL_ROUTING_MARKER.equals(slice)) {
            throw new IllegalArgumentException("invalid [_slice] value [" + slice + "]: value is reserved");
        }
        if (VALID_SLICE_VALUE_PATTERN.matcher(slice).matches() == false) {
            throw new IllegalArgumentException(
                "invalid [_slice] value ["
                    + slice
                    + "]: only [a-zA-Z0-9._:-] are allowed and max length is ["
                    + MAX_SLICE_VALUE_LENGTH
                    + "]"
            );
        }
    }

    /**
     * Parses and validates the REST-level {@code routing} and {@code _slice} parameters.
     * Returns the effective routing value and whether it was provided via {@code _slice}.
     */
    public static ParsedRouting parseRoutingOrSliceWithProvenance(RestRequest request) {
        final String routing = request.param("routing");
        final String slice = request.param(PARAM_NAME);
        if (slice != null && SLICE_FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalArgumentException("request does not support [_slice]");
        }
        if (slice != null) {
            validateUserSliceValue(slice);
        }
        if (slice != null && routing != null) {
            throw new IllegalArgumentException("[routing] is not allowed together with [_slice]");
        }
        return new ParsedRouting(slice != null ? slice : routing, slice != null);
    }

}
