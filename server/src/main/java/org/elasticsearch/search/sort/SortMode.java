/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Elasticsearch supports sorting by array or multi-valued fields. The SortMode option controls what array value is picked
 * for sorting the document it belongs to. The mode option can have the following values:
 * <ul>
 * <li>min - Pick the lowest value.</li>
 * <li>max - Pick the highest value.</li>
 * <li>sum - Use the sum of all values as sort value. Only applicable for number based array fields.</li>
 * <li>avg - Use the average of all values as sort value. Only applicable for number based array fields.</li>
 * <li>median - Use the median of all values as sort value. Only applicable for number based array fields.</li>
 * </ul>
 */
public enum SortMode implements Writeable {
    /** pick the lowest value **/
    MIN,
    /** pick the highest value **/
    MAX,
    /** Use the sum of all values as sort value. Only applicable for number based array fields. **/
    SUM,
    /** Use the average of all values as sort value. Only applicable for number based array fields. **/
    AVG,
    /** Use the median of all values as sort value. Only applicable for number based array fields. **/
    MEDIAN;

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static SortMode readFromStream(StreamInput in) throws IOException {
        return in.readEnum(SortMode.class);
    }

    public static SortMode fromString(final String str) {
        Objects.requireNonNull(str, "input string is null");
        return switch (str.toLowerCase(Locale.ROOT)) {
            case "min" -> MIN;
            case "max" -> MAX;
            case "sum" -> SUM;
            case "avg" -> AVG;
            case "median" -> MEDIAN;
            default -> throw new IllegalArgumentException("Unknown SortMode [" + str + "]");
        };
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
