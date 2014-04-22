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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.support.format.ValueParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class ExtendedBounds {

    Long min;
    Long max;

    String minAsStr;
    String maxAsStr;

    ExtendedBounds() {} //for serialization

    ExtendedBounds(Long min, Long max) {
        this.min = min;
        this.max = max;
    }

    void processAndValidate(String aggName, SearchContext context, ValueParser parser) {
        assert parser != null;
        if (minAsStr != null) {
            min = parser.parseLong(minAsStr, context);
        }
        if (maxAsStr != null) {
            max = parser.parseLong(maxAsStr, context);
        }
        if (min != null && max != null && min.compareTo(max) > 0) {
            throw new SearchParseException(context, "[extended_bounds.min][" + min + "] cannot be greater than " +
                    "[extended_bounds.max][" + max + "] for histogram aggregation [" + aggName + "]");
        }
    }

    ExtendedBounds round(Rounding rounding) {
        return new ExtendedBounds(min != null ? rounding.round(min) : null, max != null ? rounding.round(max) : null);
    }

    void writeTo(StreamOutput out) throws IOException {
        if (min != null) {
            out.writeBoolean(true);
            out.writeLong(min);
        } else {
            out.writeBoolean(false);
        }
        if (max != null) {
            out.writeBoolean(true);
            out.writeLong(max);
        } else {
            out.writeBoolean(false);
        }
    }

    static ExtendedBounds readFrom(StreamInput in) throws IOException {
        ExtendedBounds bounds = new ExtendedBounds();
        if (in.readBoolean()) {
            bounds.min = in.readLong();
        }
        if (in.readBoolean()) {
            bounds.max = in.readLong();
        }
        return bounds;
    }
}
