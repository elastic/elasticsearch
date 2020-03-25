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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.io.IOException;

public abstract class ParsedGeoGridBucket extends ParsedMultiBucketAggregation.ParsedBucket implements GeoGrid.Bucket {

    protected String hashAsString;

    @Override
    protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
        return builder.field(Aggregation.CommonFields.KEY.getPreferredName(), hashAsString);
    }
}
