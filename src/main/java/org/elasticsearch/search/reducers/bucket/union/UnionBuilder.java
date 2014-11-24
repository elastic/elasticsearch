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

package org.elasticsearch.search.reducers.bucket.union;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.reducers.ReductionBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnionBuilder extends ReductionBuilder<UnionBuilder> {

    private List<String> paths = new ArrayList<>();

    public UnionBuilder(String name) {
        super(name, InternalUnion.TYPE.name());
    }

    public UnionBuilder path(String path) {
        paths.add(path);
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (paths.size() != 0) {
            builder.startArray(UnionParser.BUCKETS_FIELD.getPreferredName());
            for (String path : paths) {
                builder.value(path);
            }
            builder.endArray();
        }

        builder.endObject();
        return builder;
    }

}
