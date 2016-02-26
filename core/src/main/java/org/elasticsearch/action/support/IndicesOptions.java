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


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.lenientNodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;

/**
 * Controls how to deal with unavailable concrete indices (closed or missing), how wildcard expressions are expanded
 * to actual indices (all, closed or open indices) and how to deal with wildcard expressions that resolve to no indices.
 */
public class IndicesOptions {

    private static final byte IGNORE_UNAVAILABLE = 1;
    private static final byte ALLOW_NO_INDICES = 2;
    private static final byte EXPAND_WILDCARDS_OPEN = 4;
    private static final byte EXPAND_WILDCARDS_CLOSED = 8;
    private static final byte FORBID_ALIASES_TO_MULTIPLE_INDICES = 16;
    private static final byte FORBID_CLOSED_INDICES = 32;

    private static final byte STRICT_EXPAND_OPEN = 6;
    private static final byte LENIENT_EXPAND_OPEN = 7;
    private static final byte INCLUDE_HIDDEN_INDICES = 64;
    private static final byte STRICT_EXPAND_OPEN_CLOSED = 14;
    private static final byte STRICT_EXPAND_OPEN_FORBID_CLOSED = 38;
    private static final byte STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED = 48;

    private static final IndicesOptions LENIENT_EXPAND_OPEN_INSTANCE = new IndicesOptions(LENIENT_EXPAND_OPEN);
    private static final IndicesOptions STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_INSTANCE = new IndicesOptions(STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED);
    private static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED_INSTANCE = new IndicesOptions(STRICT_EXPAND_OPEN_FORBID_CLOSED);
    private static final IndicesOptions STRICT_EXPAND_OPEN_INSTANCE = new IndicesOptions(STRICT_EXPAND_OPEN);
    private static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED_INSTANCE = new IndicesOptions(STRICT_EXPAND_OPEN_CLOSED);

    private final int id;

    private IndicesOptions(int id) {
        this.id = id;
    }

    /**
     * @return Whether specified concrete indices should be ignored when unavailable (missing or closed)
     */
    public boolean ignoreUnavailable() {
        return (id & IGNORE_UNAVAILABLE) != 0;
    }

    /**
     * @return Whether to ignore if a wildcard expression resolves to no concrete indices.
     *         The `_all` string or empty list of indices count as wildcard expressions too.
     *         Also when an alias points to a closed index this option decides if no concrete indices
     *         are allowed.
     */
    public boolean allowNoIndices() {
        return (id & ALLOW_NO_INDICES) != 0;
    }

    /**
     * @return Whether wildcard expressions should get expanded to open indices
     */
    public boolean expandWildcardsOpen() {
        return (id & EXPAND_WILDCARDS_OPEN) != 0;
    }

    /**
     * @return Whether wildcard expressions should get expanded to closed indices
     */
    public boolean expandWildcardsClosed() {
        return (id & EXPAND_WILDCARDS_CLOSED) != 0;
    }

    /**
     * @return Whether execution on closed indices is allowed.
     */
    public boolean forbidClosedIndices() {
        return (id & FORBID_CLOSED_INDICES) != 0;
    }

    /**
     *
     * @return Whether expressions should include hidden indices
     */
    public boolean includeHidden() { return (id & INCLUDE_HIDDEN_INDICES) != 0;}

    /**
     * @return whether aliases pointing to multiple indices are allowed
     */
    public boolean allowAliasesToMultipleIndices() {
        //true is default here, for bw comp we keep the first 16 values
        //in the array same as before + the default value for the new flag
        return (id & FORBID_ALIASES_TO_MULTIPLE_INDICES) == 0;
    }

    public void writeIndicesOptions(StreamOutput out) throws IOException {
        out.writeInt(id);
    }

    public static IndicesOptions readIndicesOptions(StreamInput in) throws IOException {
        //if we read from a node that doesn't support the newly added flag (allowAliasesToMultipleIndices)
        //we just receive the old corresponding value with the new flag set to true (default)
        int id = in.readInt();
        return new IndicesOptions(id);
    }

    public static IndicesOptions fromOptions(boolean ignoreUnavailable, boolean allowNoIndices, boolean expandToOpenIndices, boolean expandToClosedIndices) {
        return fromOptions(ignoreUnavailable, allowNoIndices, expandToOpenIndices, expandToClosedIndices, true, false, false);
    }

    public static IndicesOptions fromOptions(boolean ignoreUnavailable, boolean allowNoIndices, boolean expandToOpenIndices, boolean expandToClosedIndices, IndicesOptions defaultOptions) {
        return fromOptions(ignoreUnavailable, allowNoIndices, expandToOpenIndices, expandToClosedIndices, defaultOptions.allowAliasesToMultipleIndices(), defaultOptions.forbidClosedIndices(), defaultOptions.includeHidden());
    }

    public static IndicesOptions fromOptions(boolean ignoreUnavailable, boolean allowNoIndices, boolean expandToOpenIndices, boolean expandToClosedIndices, boolean allowAliasesToMultipleIndices, boolean forbidClosedIndices, boolean includeHiddenIndices) {
        int id = toId(ignoreUnavailable, allowNoIndices, expandToOpenIndices, expandToClosedIndices, allowAliasesToMultipleIndices, forbidClosedIndices, includeHiddenIndices);
        switch (id) {
            case LENIENT_EXPAND_OPEN:
                return LENIENT_EXPAND_OPEN_INSTANCE;
            case STRICT_EXPAND_OPEN:
                return STRICT_EXPAND_OPEN_INSTANCE;
            case STRICT_EXPAND_OPEN_CLOSED:
                return STRICT_EXPAND_OPEN_CLOSED_INSTANCE;
            case STRICT_EXPAND_OPEN_FORBID_CLOSED:
                return STRICT_EXPAND_OPEN_FORBID_CLOSED_INSTANCE;
            case STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED:
                return STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_INSTANCE;
            default:
                return new IndicesOptions(id);
        }
    }

    public static IndicesOptions fromRequest(RestRequest request, IndicesOptions defaultSettings) {
        return fromParameters(
                request.param("expand_wildcards"),
                request.param("ignore_unavailable"),
                request.param("allow_no_indices"),
                request.param("include_hidden"),
                defaultSettings);
    }

    public static IndicesOptions fromMap(Map<String, Object> map, IndicesOptions defaultSettings) {
        return fromParameters(
                map.containsKey("expand_wildcards") ? map.get("expand_wildcards") : map.get("expandWildcards"),
                map.containsKey("ignore_unavailable") ? map.get("ignore_unavailable") : map.get("ignoreUnavailable"),
                map.containsKey("allow_no_indices") ? map.get("allow_no_indices") : map.get("allowNoIndices"),
                map.getOrDefault("include_hidden", map.getOrDefault("includeHidden", "false")),
                defaultSettings);
    }

    /**
     * Returns true if the name represents a valid name for one of the indices option
     * false otherwise
     */
    public static boolean isIndicesOptions(String name) {
        switch (name) {
            case "expand_wildcards":
            case "expandWildcards":
            case "ignore_unavailable":
            case "ignoreUnavailable":
            case "allow_no_indices":
            case "allowNoIndices":
            case "include_hidden":
                return true;
            default:
                return false;
        }
    }

    public static IndicesOptions fromParameters(Object wildcardsString, Object ignoreUnavailableString, Object allowNoIndicesString, Object includeHidden, IndicesOptions defaultSettings) {
        if (wildcardsString == null && ignoreUnavailableString == null && allowNoIndicesString == null && includeHidden == null) {
            return defaultSettings;
        }

        boolean expandWildcardsOpen = false;
        boolean expandWildcardsClosed = false;
        if (wildcardsString == null) {
            expandWildcardsOpen = defaultSettings.expandWildcardsOpen();
            expandWildcardsClosed = defaultSettings.expandWildcardsClosed();
        } else {
            String[] wildcards = nodeStringArrayValue(wildcardsString);
            for (String wildcard : wildcards) {
                if ("open".equals(wildcard)) {
                    expandWildcardsOpen = true;
                } else if ("closed".equals(wildcard)) {
                    expandWildcardsClosed = true;
                } else if ("none".equals(wildcard)) {
                    expandWildcardsOpen = false;
                    expandWildcardsClosed = false;
                } else if ("all".equals(wildcard)) {
                    expandWildcardsOpen = true;
                    expandWildcardsClosed = true;
                } else {
                    throw new IllegalArgumentException("No valid expand wildcard value [" + wildcard + "]");
                }
            }
        }

        //note that allowAliasesToMultipleIndices is not exposed, always true (only for internal use)
        return fromOptions(
                lenientNodeBooleanValue(ignoreUnavailableString, defaultSettings.ignoreUnavailable()),
                lenientNodeBooleanValue(allowNoIndicesString, defaultSettings.allowNoIndices()),
                expandWildcardsOpen,
                expandWildcardsClosed,
                defaultSettings.allowAliasesToMultipleIndices(),
                defaultSettings.forbidClosedIndices(),
                lenientNodeBooleanValue(includeHidden, defaultSettings.includeHidden())
        );
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices and
     *         allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpandOpen() {
        return STRICT_EXPAND_OPEN_INSTANCE;
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices,
     *         allows that no indices are resolved from wildcard expressions (not returning an error) and forbids the
     *         use of closed indices by throwing an error.
     */
    public static IndicesOptions strictExpandOpenAndForbidClosed() {
        return STRICT_EXPAND_OPEN_FORBID_CLOSED_INSTANCE;
    }

    /**
     * @return indices option that requires every specified index to exist, expands wildcards to both open and closed
     * indices and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpand() {
        return STRICT_EXPAND_OPEN_CLOSED_INSTANCE;
    }

    /**
     * @return indices option that requires each specified index or alias to exist, doesn't expand wildcards and
     * throws error if any of the aliases resolves to multiple indices
     */
    public static IndicesOptions strictSingleIndexNoExpandForbidClosed() {
        return STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_INSTANCE;
    }

    /**
     * @return indices options that ignores unavailable indices, expands wildcards only to open indices and
     *         allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpandOpen() {
        return LENIENT_EXPAND_OPEN_INSTANCE;
    }

    private static int toId(boolean ignoreUnavailable, boolean allowNoIndices, boolean wildcardExpandToOpen,
                             boolean wildcardExpandToClosed, boolean allowAliasesToMultipleIndices, boolean forbidClosedIndices, boolean includeHidden) {
        int id = 0;
        if (ignoreUnavailable) {
            id |= IGNORE_UNAVAILABLE;
        }
        if (allowNoIndices) {
            id |= ALLOW_NO_INDICES;
        }
        if (wildcardExpandToOpen) {
            id |= EXPAND_WILDCARDS_OPEN;
        }
        if (wildcardExpandToClosed) {
            id |= EXPAND_WILDCARDS_CLOSED;
        }
        //true is default here, for bw comp we keep the first 16 values
        //in the array same as before + the default value for the new flag
        if (!allowAliasesToMultipleIndices) {
            id |= FORBID_ALIASES_TO_MULTIPLE_INDICES;
        }
        if (forbidClosedIndices) {
            id |= FORBID_CLOSED_INDICES;
        }

        if (includeHidden) {
            id |= INCLUDE_HIDDEN_INDICES;
        }
        return id;
    }

    @Override
    public String toString() {
        return "IndicesOptions[" +
                "id=" + id +
                ", ignore_unavailable=" + ignoreUnavailable() +
                ", allow_no_indices=" + allowNoIndices() +
                ", expand_wildcards_open=" + expandWildcardsOpen() +
                ", expand_wildcards_closed=" + expandWildcardsClosed() +
                ", allow_alisases_to_multiple_indices=" + allowAliasesToMultipleIndices() +
                ", forbid_closed_indices=" + forbidClosedIndices() +
                ", include_hidden=" + includeHidden() +
                ']';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndicesOptions that = (IndicesOptions) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
