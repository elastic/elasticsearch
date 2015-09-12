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

package org.elasticsearch.search.aggregations.bucket.nested;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;

/**
 * Builder for the {@link Nested} aggregation.
 */
public class NestedBuilder extends AggregationBuilder<NestedBuilder> {

    private String path;

    /**
     * Sole constructor.
     */
    public NestedBuilder(String name) {
        super(name, InternalNested.TYPE.name());
    }

    /**
     * Set the path to use for this nested aggregation. The path must match
     * the path to a nested object in the mappings. This parameter is
     * compulsory.
     */
    public NestedBuilder path(String path) {
        this.path = path;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (path == null) {
            throw new SearchSourceBuilderException("nested path must be set on nested aggregation [" + getName() + "]");
        }
        builder.field("path", path);
        return builder.endObject();
    }
}
