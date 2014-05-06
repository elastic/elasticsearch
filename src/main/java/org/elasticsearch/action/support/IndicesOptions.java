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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

/**
 * Controls how to deal when concrete indices are unavailable (closed & missing), to what wildcard expression expand
 * (all, closed or open indices) and how to deal when a wildcard expression resolves into no concrete indices.
 */
public class IndicesOptions {

    private static final IndicesOptions[] VALUES;

    static {
        byte max = 1 << 4;
        VALUES = new IndicesOptions[max];
        for (byte id = 0; id < max; id++) {
            VALUES[id] = new IndicesOptions(id);
        }
    }

    private final byte id;

    private IndicesOptions(byte id) {
        this.id = id;
    }

    /**
     * @return Whether specified concrete indices should be ignored when unavailable (missing or closed)
     */
    public boolean ignoreUnavailable() {
        return (id & 1) != 0;
    }

    /**
     * @return Whether to ignore if a wildcard indices expression resolves into no concrete indices.
     *         The `_all` string or when no indices have been specified also count as wildcard expressions.
     */
    public boolean allowNoIndices() {
        return (id & 2) != 0;
    }

    /**
     * @return Whether wildcard indices expressions should expanded into open indices should be
     */
    public boolean expandWildcardsOpen() {
        return (id & 4) != 0;
    }

    /**
     * @return Whether wildcard indices expressions should expanded into closed indices should be
     */
    public boolean expandWildcardsClosed() {
        return (id & 8) != 0;
    }

    public void writeIndicesOptions(StreamOutput out) throws IOException {
        out.write(id);
    }

    public static IndicesOptions readIndicesOptions(StreamInput in) throws IOException {
        byte id = in.readByte();
        if (id >= VALUES.length) {
            throw new ElasticsearchIllegalArgumentException("No valid missing index type id: " + id);
        }
        return VALUES[id];
    }

    public static IndicesOptions fromOptions(boolean ignoreUnavailable, boolean allowNoIndices, boolean expandToOpenIndices, boolean expandToClosedIndices) {
        byte id = toByte(ignoreUnavailable, allowNoIndices, expandToOpenIndices, expandToClosedIndices);
        return VALUES[id];
    }

    public static IndicesOptions fromRequest(RestRequest request, IndicesOptions defaultSettings) {
        String sWildcards = request.param("expand_wildcards");
        String sIgnoreUnavailable = request.param("ignore_unavailable");
        String sAllowNoIndices = request.param("allow_no_indices");
        if (sWildcards == null && sIgnoreUnavailable == null && sAllowNoIndices == null) {
            return defaultSettings;
        }

        boolean expandWildcardsOpen = defaultSettings.expandWildcardsOpen();
        boolean expandWildcardsClosed = defaultSettings.expandWildcardsClosed();
        if (sWildcards != null) {
            String[] wildcards = Strings.splitStringByCommaToArray(sWildcards);
            for (String wildcard : wildcards) {
                if ("open".equals(wildcard)) {
                    expandWildcardsOpen = true;
                } else if ("closed".equals(wildcard)) {
                    expandWildcardsClosed = true;
                } else {
                    throw new ElasticsearchIllegalArgumentException("No valid expand wildcard value [" + wildcard + "]");
                }
            }
        }

        return fromOptions(
                toBool(sIgnoreUnavailable, defaultSettings.ignoreUnavailable()),
                toBool(sAllowNoIndices, defaultSettings.allowNoIndices()),
                expandWildcardsOpen,
                expandWildcardsClosed
        );
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices and
     *         allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpandOpen() {
        return VALUES[6];
    }


    /**
     * @return indices option that requires every specified index to exist, expands wildcards to both open and closed
     * indices and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpand() {
        return VALUES[14];
    }

    /**
     * @return indices options that ignores unavailable indices, expands wildcards only to open indices and
     *         allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpandOpen() {
        return VALUES[7];
    }

    private static byte toByte(boolean ignoreUnavailable, boolean allowNoIndices, boolean wildcardExpandToOpen, boolean wildcardExpandToClosed) {
        byte id = 0;
        if (ignoreUnavailable) {
            id |= 1;
        }
        if (allowNoIndices) {
            id |= 2;
        }
        if (wildcardExpandToOpen) {
            id |= 4;
        }
        if (wildcardExpandToClosed) {
            id |= 8;
        }
        return id;
    }

    private static boolean toBool(String sValue, boolean defaultValue) {
        if (sValue == null) {
            return defaultValue;
        }
        return !(sValue.equals("false") || sValue.equals("0") || sValue.equals("off"));
    }

}
