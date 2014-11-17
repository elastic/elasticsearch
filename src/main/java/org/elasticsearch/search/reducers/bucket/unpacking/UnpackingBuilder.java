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

package org.elasticsearch.search.reducers.bucket.unpacking;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.reducers.ReductionBuilder;

import java.io.IOException;

public class UnpackingBuilder extends ReductionBuilder<UnpackingBuilder> {

    private String path = null;
    private String unpackPath = null;

    public UnpackingBuilder(String name) {
        super(name, InternalUnpacking.TYPE.name());
    }

    public UnpackingBuilder path(String path) {
        this.path = path;
        return this;
    }

    public UnpackingBuilder unpackPath(String unpackPath) {
        this.unpackPath = unpackPath;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (path != null) {
            builder.field(UnpackingParser.BUCKETS_FIELD.getPreferredName(), path);
        }

        if (unpackPath != null) {
            builder.field(UnpackingParser.UNPACK_PATH_FIELD.getPreferredName(), unpackPath);
        }

        builder.endObject();
        return builder;
    }

}
