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
        out.writeVInt(ordinal());
    }

    public static SortMode readFromStream(StreamInput in) throws IOException {
        int ordinal = in.readVInt();
        if (ordinal < 0 || ordinal >= values().length) {
            throw new IOException("Unknown SortMode ordinal [" + ordinal + "]");
        }
        return values()[ordinal];
    }

    public static SortMode fromString(final String str) {
        Objects.requireNonNull(str, "input string is null");
        switch (str.toLowerCase(Locale.ROOT)) {
            case ("min"):
                return MIN;
            case ("max"):
                return MAX;
            case ("sum"):
                return SUM;
            case ("avg"):
                return AVG;
            case ("median"):
                return MEDIAN;
            default:
                throw new IllegalArgumentException("Unknown SortMode [" + str + "]");
        }
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}