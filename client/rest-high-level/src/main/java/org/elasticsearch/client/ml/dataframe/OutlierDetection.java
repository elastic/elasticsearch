/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class OutlierDetection implements DataFrameAnalysis {

    public static OutlierDetection fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    static final ParseField NAME = new ParseField("outlier_detection");
    static final ParseField N_NEIGHBORS = new ParseField("n_neighbors");
    static final ParseField METHOD = new ParseField("method");

    private static ConstructingObjectParser<OutlierDetection, Void> PARSER =
        new ConstructingObjectParser<>(NAME.getPreferredName(), true,
            (args) -> {
                Integer nNeighbors = (Integer) args[0];
                Method method = (Method) args[1];
                return new OutlierDetection(nNeighbors, method);
            });

    private final Integer nNeighbors;
    private final Method method;

    static {
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), N_NEIGHBORS);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return Method.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, METHOD, ObjectParser.ValueType.STRING);
    }

    /**
     * Constructs the outlier detection configuration
     * @param nNeighbors The number of neighbors. Leave unspecified for dynamic detection.
     * @param method The method. Leave unspecified for a dynamic mixture of methods.
     */
    public OutlierDetection(@Nullable Integer nNeighbors, @Nullable Method method) {
        if (nNeighbors != null && nNeighbors <= 0) {
            throw new IllegalArgumentException("[" + N_NEIGHBORS.getPreferredName() + "] must be a positive integer");
        }

        this.nNeighbors = nNeighbors;
        this.method = method;
    }

    /**
     * Constructs the default outlier detection configuration
     */
    public OutlierDetection() {
        this(null, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (nNeighbors != null) {
            builder.field(N_NEIGHBORS.getPreferredName(), nNeighbors);
        }
        if (method != null) {
            builder.field(METHOD.getPreferredName(), method);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OutlierDetection other = (OutlierDetection) o;
        return Objects.equals(nNeighbors, other.nNeighbors)
            && Objects.equals(method, other.method);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nNeighbors, method);
    }

    @Override
    public String getName() { return NAME.getPreferredName(); }

    @Override
    public Map<String, Object> getParams() {
        Map<String, Object> params = new HashMap<>();
        if (nNeighbors != null) {
            params.put(N_NEIGHBORS.getPreferredName(), nNeighbors);
        }
        if (method != null) {
            params.put(METHOD.getPreferredName(), method);
        }
        return params;
    }

    public enum Method {
        LOF, LDOF, DISTANCE_KTH_NN, DISTANCE_KNN;

        public static Method fromString(String value) {
            return Method.valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
