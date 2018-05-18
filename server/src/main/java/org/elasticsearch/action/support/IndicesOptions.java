/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.support;


import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;

/**
 * Controls how to deal with unavailable concrete indices (closed or missing), how wildcard expressions are expanded
 * to actual indices (all, closed or open indices) and how to deal with wildcard expressions that resolve to no indices.
 */
public class IndicesOptions {

    public enum WildcardStates {
        OPEN,
        CLOSED;

        public static final EnumSet<WildcardStates> NONE = EnumSet.noneOf(WildcardStates.class);

        public static EnumSet<WildcardStates> parseParameter(Object value, EnumSet<WildcardStates> defaultStates) {
            if (value == null) {
                return defaultStates;
            }

            Set<WildcardStates> states = new HashSet<>();
            String[] wildcards = nodeStringArrayValue(value);
            for (String wildcard : wildcards) {
                if ("open".equals(wildcard)) {
                    states.add(OPEN);
                } else if ("closed".equals(wildcard)) {
                    states.add(CLOSED);
                } else if ("none".equals(wildcard)) {
                    states.clear();
                } else if ("all".equals(wildcard)) {
                    states.add(OPEN);
                    states.add(CLOSED);
                } else {
                    throw new IllegalArgumentException("No valid expand wildcard value [" + wildcard + "]");
                }
            }

            return states.isEmpty() ? NONE : EnumSet.copyOf(states);
        }
    }

    public enum Option {
        IGNORE_UNAVAILABLE,
        IGNORE_ALIASES,
        ALLOW_NO_INDICES,
        FORBID_ALIASES_TO_MULTIPLE_INDICES,
        FORBID_CLOSED_INDICES;

        public static final EnumSet<Option> NONE = EnumSet.noneOf(Option.class);
    }

    public static final IndicesOptions STRICT_EXPAND_OPEN =
        new IndicesOptions(EnumSet.of(Option.ALLOW_NO_INDICES), EnumSet.of(WildcardStates.OPEN));
    public static final IndicesOptions LENIENT_EXPAND_OPEN =
        new IndicesOptions(EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE), EnumSet.of(WildcardStates.OPEN));
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED =
        new IndicesOptions(EnumSet.of(Option.ALLOW_NO_INDICES), EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED));
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED =
        new IndicesOptions(EnumSet.of(Option.ALLOW_NO_INDICES, Option.FORBID_CLOSED_INDICES), EnumSet.of(WildcardStates.OPEN));
    public static final IndicesOptions STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED =
        new IndicesOptions(EnumSet.of(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES, Option.FORBID_CLOSED_INDICES),
            EnumSet.noneOf(WildcardStates.class));

    private final EnumSet<Option> options;
    private final EnumSet<WildcardStates> expandWildcards;

    public IndicesOptions(EnumSet<Option> options, EnumSet<WildcardStates> expandWildcards) {
        this.options = options;
        this.expandWildcards = expandWildcards;
    }

    private IndicesOptions(Collection<Option> options, Collection<WildcardStates> expandWildcards) {
        this(options.isEmpty() ? Option.NONE : EnumSet.copyOf(options),
            expandWildcards.isEmpty() ? WildcardStates.NONE : EnumSet.copyOf(expandWildcards));
    }

    // Package visible for testing
    static IndicesOptions fromByte(final byte id) {
        // IGNORE_UNAVAILABLE = 1;
        // ALLOW_NO_INDICES = 2;
        // EXPAND_WILDCARDS_OPEN = 4;
        // EXPAND_WILDCARDS_CLOSED = 8;
        // FORBID_ALIASES_TO_MULTIPLE_INDICES = 16;
        // FORBID_CLOSED_INDICES = 32;
        // IGNORE_ALIASES = 64;

        Set<Option> opts = new HashSet<>();
        Set<WildcardStates> wildcards = new HashSet<>();
        if ((id & 1) != 0) {
            opts.add(Option.IGNORE_UNAVAILABLE);
        }
        if ((id & 2) != 0) {
            opts.add(Option.ALLOW_NO_INDICES);
        }
        if ((id & 4) != 0) {
            wildcards.add(WildcardStates.OPEN);
        }
        if ((id & 8) != 0) {
            wildcards.add(WildcardStates.CLOSED);
        }
        if ((id & 16) != 0) {
            opts.add(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES);
        }
        if ((id & 32) != 0) {
            opts.add(Option.FORBID_CLOSED_INDICES);
        }
        if ((id & 64) != 0) {
            opts.add(Option.IGNORE_ALIASES);
        }
        return new IndicesOptions(opts, wildcards);
    }

    /**
     * See: {@link #fromByte(byte)}
     */
    private static byte toByte(IndicesOptions options) {
        byte id = 0;
        if (options.ignoreUnavailable()) {
            id |= 1;
        }
        if (options.allowNoIndices()) {
            id |= 2;
        }
        if (options.expandWildcardsOpen()) {
            id |= 4;
        }
        if (options.expandWildcardsClosed()) {
            id |= 8;
        }
        // true is default here, for bw comp we keep the first 16 values
        // in the array same as before + the default value for the new flag
        if (options.allowAliasesToMultipleIndices() == false) {
            id |= 16;
        }
        if (options.forbidClosedIndices()) {
            id |= 32;
        }
        if (options.ignoreAliases()) {
            id |= 64;
        }
        return id;
    }

    private static final IndicesOptions[] OLD_VALUES;

    static {
        short max = 1 << 7;
        OLD_VALUES = new IndicesOptions[max];
        for (short id = 0; id < max; id++) {
            OLD_VALUES[id] = IndicesOptions.fromByte((byte)id);
        }
    }

    /**
     * @return Whether specified concrete indices should be ignored when unavailable (missing or closed)
     */
    public boolean ignoreUnavailable() {
        return options.contains(Option.IGNORE_UNAVAILABLE);
    }

    /**
     * @return Whether to ignore if a wildcard expression resolves to no concrete indices.
     *         The `_all` string or empty list of indices count as wildcard expressions too.
     *         Also when an alias points to a closed index this option decides if no concrete indices
     *         are allowed.
     */
    public boolean allowNoIndices() {
        return options.contains(Option.ALLOW_NO_INDICES);
    }

    /**
     * @return Whether wildcard expressions should get expanded to open indices
     */
    public boolean expandWildcardsOpen() {
        return expandWildcards.contains(WildcardStates.OPEN);
    }

    /**
     * @return Whether wildcard expressions should get expanded to closed indices
     */
    public boolean expandWildcardsClosed() {
        return expandWildcards.contains(WildcardStates.CLOSED);
    }

    /**
     * @return Whether execution on closed indices is allowed.
     */
    public boolean forbidClosedIndices() {
        return options.contains(Option.FORBID_CLOSED_INDICES);
    }

    /**
     * @return whether aliases pointing to multiple indices are allowed
     */
    public boolean allowAliasesToMultipleIndices() {
        // true is default here, for bw comp we keep the first 16 values
        // in the array same as before + the default value for the new flag
        return options.contains(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES) == false;
    }

    /**
     * @return whether aliases should be ignored (when resolving a wildcard)
     */
    public boolean ignoreAliases() {
        return options.contains(Option.IGNORE_ALIASES);
    }

    public void writeIndicesOptions(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
            out.writeEnumSet(options);
            out.writeEnumSet(expandWildcards);
        } else {
            out.write(IndicesOptions.toByte(this));
        }
    }

    public static IndicesOptions readIndicesOptions(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
            return new IndicesOptions(in.readEnumSet(Option.class), in.readEnumSet(WildcardStates.class));
        } else {
            byte id = in.readByte();
            if (id >= OLD_VALUES.length) {
                throw new IllegalArgumentException("No valid missing index type id: " + id);
            }
            return OLD_VALUES[id];
        }
    }

    public static IndicesOptions fromOptions(boolean ignoreUnavailable, boolean allowNoIndices, boolean expandToOpenIndices, boolean expandToClosedIndices) {
        return fromOptions(ignoreUnavailable, allowNoIndices, expandToOpenIndices, expandToClosedIndices, true, false, false);
    }

    public static IndicesOptions fromOptions(boolean ignoreUnavailable, boolean allowNoIndices, boolean expandToOpenIndices, boolean expandToClosedIndices, IndicesOptions defaultOptions) {
        return fromOptions(ignoreUnavailable, allowNoIndices, expandToOpenIndices, expandToClosedIndices, defaultOptions.allowAliasesToMultipleIndices(), defaultOptions.forbidClosedIndices(), defaultOptions.ignoreAliases());
    }

    public static IndicesOptions fromOptions(boolean ignoreUnavailable, boolean allowNoIndices, boolean expandToOpenIndices,
            boolean expandToClosedIndices, boolean allowAliasesToMultipleIndices, boolean forbidClosedIndices, boolean ignoreAliases) {
        final Set<Option> opts = new HashSet<>();
        final Set<WildcardStates> wildcards = new HashSet<>();

        if (ignoreUnavailable) {
            opts.add(Option.IGNORE_UNAVAILABLE);
        }
        if (allowNoIndices) {
            opts.add(Option.ALLOW_NO_INDICES);
        }
        if (expandToOpenIndices) {
            wildcards.add(WildcardStates.OPEN);
        }
        if (expandToClosedIndices) {
            wildcards.add(WildcardStates.CLOSED);
        }
        if (allowAliasesToMultipleIndices == false) {
            opts.add(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES);
        }
        if (forbidClosedIndices) {
            opts.add(Option.FORBID_CLOSED_INDICES);
        }
        if (ignoreAliases) {
            opts.add(Option.IGNORE_ALIASES);
        }

        return new IndicesOptions(opts, wildcards);
    }

    public static IndicesOptions fromRequest(RestRequest request, IndicesOptions defaultSettings) {
        return fromParameters(
                request.param("expand_wildcards"),
                request.param("ignore_unavailable"),
                request.param("allow_no_indices"),
                defaultSettings);
    }

    public static IndicesOptions fromMap(Map<String, Object> map, IndicesOptions defaultSettings) {
        return fromParameters(
                map.containsKey("expand_wildcards") ? map.get("expand_wildcards") : map.get("expandWildcards"),
                map.containsKey("ignore_unavailable") ? map.get("ignore_unavailable") : map.get("ignoreUnavailable"),
                map.containsKey("allow_no_indices") ? map.get("allow_no_indices") : map.get("allowNoIndices"),
                defaultSettings);
    }

    /**
     * Returns true if the name represents a valid name for one of the indices option
     * false otherwise
     */
    public static boolean isIndicesOptions(String name) {
        return "expand_wildcards".equals(name) || "expandWildcards".equals(name) ||
                "ignore_unavailable".equals(name) || "ignoreUnavailable".equals(name) ||
                "allow_no_indices".equals(name) || "allowNoIndices".equals(name);
    }

    public static IndicesOptions fromParameters(Object wildcardsString, Object ignoreUnavailableString, Object allowNoIndicesString, IndicesOptions defaultSettings) {
        if (wildcardsString == null && ignoreUnavailableString == null && allowNoIndicesString == null) {
            return defaultSettings;
        }

        EnumSet<WildcardStates> wildcards = WildcardStates.parseParameter(wildcardsString, defaultSettings.expandWildcards);

        // note that allowAliasesToMultipleIndices is not exposed, always true (only for internal use)
        return fromOptions(
                nodeBooleanValue(ignoreUnavailableString, "ignore_unavailable", defaultSettings.ignoreUnavailable()),
                nodeBooleanValue(allowNoIndicesString, "allow_no_indices", defaultSettings.allowNoIndices()),
                wildcards.contains(WildcardStates.OPEN),
                wildcards.contains(WildcardStates.CLOSED),
                defaultSettings.allowAliasesToMultipleIndices(),
                defaultSettings.forbidClosedIndices(),
                defaultSettings.ignoreAliases()
        );
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices and
     *         allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpandOpen() {
        return STRICT_EXPAND_OPEN;
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices,
     *         allows that no indices are resolved from wildcard expressions (not returning an error) and forbids the
     *         use of closed indices by throwing an error.
     */
    public static IndicesOptions strictExpandOpenAndForbidClosed() {
        return STRICT_EXPAND_OPEN_FORBID_CLOSED;
    }

    /**
     * @return indices option that requires every specified index to exist, expands wildcards to both open and closed
     * indices and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpand() {
        return STRICT_EXPAND_OPEN_CLOSED;
    }

    /**
     * @return indices option that requires each specified index or alias to exist, doesn't expand wildcards and
     * throws error if any of the aliases resolves to multiple indices
     */
    public static IndicesOptions strictSingleIndexNoExpandForbidClosed() {
        return STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED;
    }

    /**
     * @return indices options that ignores unavailable indices, expands wildcards only to open indices and
     *         allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpandOpen() {
        return LENIENT_EXPAND_OPEN;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != getClass()) {
            return false;
        }

        IndicesOptions other = (IndicesOptions) obj;
        return options.equals(other.options) && expandWildcards.equals(other.expandWildcards);
    }

    @Override
    public int hashCode() {
        int result = options.hashCode();
        return 31 * result + expandWildcards.hashCode();
    }

    @Override
    public String toString() {
        return "IndicesOptions[" +
                "ignore_unavailable=" + ignoreUnavailable() +
                ", allow_no_indices=" + allowNoIndices() +
                ", expand_wildcards_open=" + expandWildcardsOpen() +
                ", expand_wildcards_closed=" + expandWildcardsClosed() +
                ", allow_aliases_to_multiple_indices=" + allowAliasesToMultipleIndices() +
                ", forbid_closed_indices=" + forbidClosedIndices() +
                ", ignore_aliases=" + ignoreAliases() +
                ']';
    }
}
