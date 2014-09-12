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
package org.elasticsearch.search.aggregations.bucket.children;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;

/**
 * Builder for the {@link Children} aggregation.
 */
public class ChildrenBuilder extends AggregationBuilder<ChildrenBuilder> {

    private String childType;

    /**
     * Sole constructor.
     */
    public ChildrenBuilder(String name) {
        super(name, InternalChildren.TYPE.name());
    }

    /**
     * Set the type of children documents. This parameter is compulsory.
     */
    public ChildrenBuilder childType(String childType) {
        this.childType = childType;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (childType == null) {
            throw new SearchSourceBuilderException("child_type must be set on children aggregation [" + getName() + "]");
        }
        builder.field("type", childType);
        return builder.endObject();
    }
}
