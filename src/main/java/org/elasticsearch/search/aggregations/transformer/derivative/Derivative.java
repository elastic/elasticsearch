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

package org.elasticsearch.search.aggregations.transformer.derivative;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface Derivative extends MultiBucketsAggregation {

    public static enum GapPolicy {
        INSERT_ZEROS((byte) 0, "insert_zeros"),
        INTERPOLATE((byte) 1, "interpolate"),
        IGNORE((byte) 2, "ignore");

        public static GapPolicy parse(SearchContext context, String text) {
            GapPolicy result = null;
            for (GapPolicy policy : values()) {
                if (policy.parseField.match(text)) {
                    if (result == null) {
                        result = policy;
                    } else {
                        throw new ElasticsearchIllegalStateException("Text can be parsed to 2 different gap policies: text=[" + text + "], "
                                + "policies=" + Arrays.asList(result, policy));
                    }
                }
            }
            if (result == null) {
                final List<String> validNames = new ArrayList<>();
                for (GapPolicy policy : values()) {
                    validNames.add(policy.getName());
                }
                throw new SearchParseException(context, "Invalid gap policy: [" + text + "], accepted values: " + validNames);
            }
            return result;
        }

        private final byte id;
        private final ParseField parseField;

        private GapPolicy(byte id, String name) {
            this.id = id;
            this.parseField = new ParseField(name);
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        public static GapPolicy readFrom(StreamInput in) throws IOException {
            byte id = in.readByte();
            for (GapPolicy gapPolicy : values()) {
                if (id == gapPolicy.id) {
                    return gapPolicy;
                }
            }
            throw new IllegalStateException("Unknown GapPolicy with id [" + id + "]");
        }

        public String getName() {
            return parseField.getPreferredName();
        }
    }

    /**
     * @return The buckets of this aggregation.
     */
    List<? extends Histogram.Bucket> getBuckets();
}
