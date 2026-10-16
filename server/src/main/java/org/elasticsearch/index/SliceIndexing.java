/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestRequest;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Centralizes slice-indexing feature gating.
 */
public final class SliceIndexing {

    private SliceIndexing() {}

    /** Slice identifier name used everywhere: request-side (path segment, per-item body field, msearch header)
     * and as the document metadata field / script-context name. */
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
     * Doc-values field holding the slice sort key, {@code BE32(sliceHash) ++ utf8(slice)}; the primary (and only
     * prepended) index sort of a slice-enabled index. Bytewise order equals {@code (unsigned hash, slice)} order, so
     * segments are laid out by hash prefix while slices with colliding hashes remain distinct, adjacent terms.
     */
    public static final String SLICE_KEY_FIELD_NAME = "_slice_key";

    /**
     * Reserved field name; nothing writes it. The slice hash needs no column of its own because it is the
     * {@link #SLICE_KEY_FIELD_NAME} prefix ({@link #sliceHashFromKey}), readable per document, per term, or per segment
     * from the key. The name stays reserved so a future hash-derived structure (for example a per-segment distinct-hash
     * summary for leaf pruning or merge partitioning) can claim it without colliding with user mappings.
     */
    public static final String SLICE_HASH_FIELD_NAME = "_slice_hash";

    private static final int SLICE_HASH_BYTES = Integer.BYTES;

    /**
     * Width of the slice prefix on an indexed term: {@link #sliceHash} as fixed-width lowercase hex.
     *
     * <p>Hex rather than the raw four bytes because the prefix is applied to a token stream as characters as well as to
     * a {@link BytesRef} as bytes, and only an ASCII rendering makes those two produce the same term. Fixed width also
     * means a search across every slice is {@code ANY^8} instead of needing a separator, and stripping is a constant
     * offset.
     */
    public static final int TERM_PREFIX_LENGTH = 8;

    /**
     * Mapped field types whose terms a slice index leaves plain because no query asks for them by value: {@code completion}
     * encodes a surface form for its transducer and is suggested over rather than searched. {@code SliceTermPrefix}
     * recognises the same fields on the way in, by the class of the Lucene field they add, so a field named here is one
     * whose terms never carry a prefix.
     */
    private static final Set<String> SELF_ENCODED_TERM_TYPES = Set.of("completion");

    /** Unsigned 32-bit Murmur3 of the slice value; the same hash shard routing uses for the routing value. */
    public static long sliceHash(String slice) {
        return Murmur3HashFunction.hash(slice) & 0xFFFFFFFFL;
    }

    /** As {@link #sliceHash(String)} for a raw UTF-8 slice value (never an encoded key). */
    public static long sliceHash(BytesRef slice) {
        return Murmur3HashFunction.hash(slice) & 0xFFFFFFFFL;
    }

    /** Encodes a {@link #SLICE_KEY_FIELD_NAME} term: big-endian unsigned hash followed by the UTF-8 slice bytes. */
    public static BytesRef encodeSliceKey(String slice) {
        final byte[] utf8 = slice.getBytes(StandardCharsets.UTF_8);
        return encodeSliceKey(sliceHash(slice), utf8, 0, utf8.length);
    }

    /** As {@link #encodeSliceKey(String)} for a raw UTF-8 slice value; never pass an already encoded key. */
    public static BytesRef encodeSliceKey(BytesRef slice) {
        return encodeSliceKey(sliceHash(slice), slice.bytes, slice.offset, slice.length);
    }

    private static BytesRef encodeSliceKey(long hash, byte[] utf8, int offset, int length) {
        final byte[] key = new byte[SLICE_HASH_BYTES + length];
        ByteUtils.writeIntBE((int) hash, key, 0);
        System.arraycopy(utf8, offset, key, SLICE_HASH_BYTES, length);
        return new BytesRef(key);
    }

    /**
     * Field names a slice-enabled index reserves: the user-facing {@link #FIELD_NAME} alias, the internal
     * {@link #SLICE_KEY_FIELD_NAME} doc-values field, and the reserved {@link #SLICE_HASH_FIELD_NAME}.
     */
    public static boolean isReservedFieldName(String name) {
        return FIELD_NAME.equals(name) || SLICE_KEY_FIELD_NAME.equals(name) || SLICE_HASH_FIELD_NAME.equals(name);
    }

    /** The unsigned hash prefix of an encoded slice key. */
    public static long sliceHashFromKey(BytesRef key) {
        assert key.length >= SLICE_HASH_BYTES : "slice key too short: " + key.length;
        return ByteUtils.readIntBE(key.bytes, key.offset) & 0xFFFFFFFFL;
    }

    /** The slice value an encoded slice key was built from. */
    public static String sliceFromKey(BytesRef key) {
        assert key.length >= SLICE_HASH_BYTES : "slice key too short: " + key.length;
        return new BytesRef(key.bytes, key.offset + SLICE_HASH_BYTES, key.length - SLICE_HASH_BYTES).utf8ToString();
    }

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
     * Whether the terms of {@code fieldName} carry a slice prefix. The index and the query side both go through here, so
     * a field this returns {@code false} for keeps its terms interleaved across slices but stays correct. Names starting
     * with {@code _} are left out: the engine looks those up without a slice, for instance {@code _id} on a get, and
     * {@code _routing}, which carries the exact slice filter a search is scoped by.
     */
    public static boolean prefixesTerms(String fieldName) {
        return fieldName.isEmpty() == false && fieldName.charAt(0) != '_';
    }

    /** Whether a field of this mapped type keeps plain terms on a slice index. See {@link #SELF_ENCODED_TERM_TYPES}. */
    public static boolean keepsPlainTerms(String typeName) {
        return SELF_ENCODED_TERM_TYPES.contains(typeName);
    }

    /**
     * The characters every indexed term of {@code slice} starts with: its {@link #sliceHash} as
     * {@link #TERM_PREFIX_LENGTH} hex digits. The hash is the one {@link #SLICE_KEY_FIELD_NAME} and shard routing use,
     * so a slice has a single identity across the index.
     *
     * <p>Two slices may share a hash and therefore a region of the terms dictionary. That costs locality for the pair
     * and nothing else: a slice search is filtered on the exact slice value, never on this prefix.
     */
    public static String termPrefix(String slice) {
        return String.format(Locale.ROOT, "%08x", sliceHash(slice));
    }

    /** {@link #termPrefix(String)} as bytes; ASCII, so one byte a character. */
    public static byte[] termPrefixBytes(String slice) {
        return termPrefix(slice).getBytes(StandardCharsets.US_ASCII);
    }

    /** Prefixes an indexed term with the slice it belongs to; see {@code SliceTermPrefix} for where this applies. */
    public static BytesRef prefixTerm(byte[] termPrefix, BytesRef term) {
        final byte[] out = new byte[termPrefix.length + term.length];
        System.arraycopy(termPrefix, 0, out, 0, termPrefix.length);
        System.arraycopy(term.bytes, term.offset, out, termPrefix.length, term.length);
        return new BytesRef(out, 0, out.length);
    }

    /** Prefixes a term for a slice given by name; see {@link #prefixTerm(byte[], BytesRef)}. */
    public static BytesRef prefixTerm(String slice, BytesRef term) {
        return prefixTerm(termPrefixBytes(slice), term);
    }

    /**
     * Removes the slice prefix written by {@link #prefixTerm}, for the paths that hand a term back to the user such as
     * term vectors. Returns the term unchanged when it is too short to carry one.
     */
    public static BytesRef stripTermPrefix(BytesRef term) {
        if (term.length < TERM_PREFIX_LENGTH) {
            return term;
        }
        return new BytesRef(term.bytes, term.offset + TERM_PREFIX_LENGTH, term.length - TERM_PREFIX_LENGTH);
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
                "invalid ["
                    + FIELD_NAME
                    + "] value ["
                    + slice
                    + "]: length ["
                    + slice.length()
                    + "] exceeds max ["
                    + MAX_SLICE_VALUE_LENGTH
                    + "]"
            );
        }
        if (SLICE_ALL.equals(slice)) {
            throw new IllegalArgumentException("invalid [" + FIELD_NAME + "] value [" + slice + "]: value is reserved");
        }
        if (VALID_SLICE_VALUE_PATTERN.matcher(slice).matches() == false) {
            throw new IllegalArgumentException(
                "invalid ["
                    + FIELD_NAME
                    + "] value ["
                    + slice
                    + "]: only [a-zA-Z0-9._:-] are allowed and max length is ["
                    + MAX_SLICE_VALUE_LENGTH
                    + "]"
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
     */
    public static ParsedRouting parseSearchRoutingOrSliceWithProvenance(RestRequest request) {
        final String routing = request.param("routing");
        if (isPathBasedSliceSearch(request)) {
            if (SLICE_FEATURE_FLAG.isEnabled() == false) {
                throw new IllegalArgumentException("request does not support [" + FIELD_NAME + "]");
            }
            if (routing != null) {
                throw new IllegalArgumentException("[routing] is not allowed together with [" + FIELD_NAME + "]");
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
     * Validates and normalizes a {@code _slice} value supplied inside the search {@code pit} object. A point-in-time search targets
     * no index, so it cannot carry the slice as a {@code /{index}/{_slice}/_search} path segment; instead the slice is provided as
     * the {@code _slice} field of the {@code pit} object. The value may be a single slice, a comma-separated list, or the reserved
     * token {@code _all}. Requires the slice feature flag to be enabled.
     */
    public static ParsedRouting parsePitSearchSlice(String slice) {
        if (SLICE_FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalArgumentException("request does not support [" + FIELD_NAME + "]");
        }
        return parseSearchPathSlice(slice);
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
     * {@code /{index}/{_slice}/_search}, {@code /{index}/{_slice}/_count}, {@code /{index}/{_slice}/_delete_by_query},
     * {@code /{index}/{_slice}/_update_by_query}, {@code /{index}/{_slice}/_search/template}, and
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
            return "_search".equals(parts[2])
                || "_count".equals(parts[2])
                || "_delete_by_query".equals(parts[2])
                || "_update_by_query".equals(parts[2]);
        }
        if (parts.length == 4) {
            return ("_search".equals(parts[2]) && "template".equals(parts[3]))
                || ("_fleet".equals(parts[2]) && "_fleet_search".equals(parts[3]));
        }
        return false;
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
                "["
                    + FIELD_NAME
                    + "] is not allowed when [index.slice.enabled] is false for "
                    + requestDescription
                    + " targeting ["
                    + target
                    + "]"
            );
        }
        if (sliceEnabled && routingFromSlice == false) {
            if (routing != null) {
                throw new IllegalArgumentException(
                    "[routing] is not allowed when [index.slice.enabled] is true for "
                        + requestDescription
                        + " targeting ["
                        + target
                        + "], use ["
                        + FIELD_NAME
                        + "] instead"
                );
            }
            throw new IllegalArgumentException(
                "["
                    + FIELD_NAME
                    + "] is required when [index.slice.enabled] is true for "
                    + requestDescription
                    + " targeting ["
                    + target
                    + "]"
            );
        }
    }

    /**
     * Validates request-level slice/routing requirements and resolves effective routing for search-style APIs.
     * When {@code anySliceEnabled} is true and no {@code _slice} parameter was provided, the request is treated
     * as {@code _slice=_all} (routing is left unrestricted, covering all slices). A {@code _slice} is accepted even when no
     * targeted index is slice-enabled: indices without slices hold no document of any slice, so they match nothing.
     */
    public static String validateAndResolveSliceRoutingRequirement(
        boolean anySliceEnabled,
        boolean routingFromSlice,
        String routing,
        String requestedSlice,
        String requestDescription,
        String target
    ) {
        if (anySliceEnabled && routingFromSlice == false && routing != null) {
            throw new IllegalArgumentException(
                "[routing] is not allowed when [index.slice.enabled] is true for "
                    + requestDescription
                    + " targeting ["
                    + target
                    + "], use ["
                    + FIELD_NAME
                    + "] instead"
            );
        }
        if (routingFromSlice) {
            return SLICE_ALL.equals(requestedSlice) ? null : requestedSlice;
        }
        return routing;
    }

}
