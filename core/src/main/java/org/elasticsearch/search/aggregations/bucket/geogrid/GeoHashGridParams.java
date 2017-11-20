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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.ParseField;

/**
 * Encapsulates relevant parameter defaults and validations for the geo hash grid aggregation.
 */
final class GeoHashGridParams {

    /* recognized field names in JSON */
    static final ParseField FIELD_PRECISION = new ParseField("precision");
    static final ParseField FIELD_SIZE = new ParseField("size");
    static final ParseField FIELD_SHARD_SIZE = new ParseField("shard_size");


    static int checkPrecision(int precision) {
        if ((precision < 1) || (precision > 12)) {
            throw new IllegalArgumentException("Invalid geohash aggregation precision of " + precision
                    + ". Must be between 1 and 12.");
        }
        return precision;
    }

    private GeoHashGridParams() {
        throw new AssertionError("No instances intended");
    }
}
