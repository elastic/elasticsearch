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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * An enum representing the methods for calculating percentiles
 */
public enum PercentilesMethod implements Writeable {
    /**
     * The TDigest method for calculating percentiles
     */
    TDIGEST("tdigest", "TDigest", "TDIGEST"),
    /**
     * The HDRHistogram method of calculating percentiles
     */
    HDR("hdr", "HDR");

    private final ParseField parseField;

    PercentilesMethod(String name, String... deprecatedNames) {
        this.parseField = new ParseField(name, deprecatedNames);
    }

    /**
     * @return the name of the method
     */
    public ParseField getParseField() {
        return parseField;
    }

    public static PercentilesMethod readFromStream(StreamInput in) throws IOException {
        return in.readEnum(PercentilesMethod.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    @Override
    public String toString() {
        return parseField.getPreferredName();
    }
}
