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

package org.elasticsearch.action.admin.indices.dangling;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ListDanglingIndicesResponse extends ActionResponse implements StatusToXContentObject {
    private final List<DanglingIndexInfo> indexInfo;

    public ListDanglingIndicesResponse(List<DanglingIndexInfo> indexInfo) {
        this.indexInfo = indexInfo;
    }

    public ListDanglingIndicesResponse(StreamInput in) throws IOException {
        super(in);
        this.indexInfo = Collections.emptyList();
    }

    public List<DanglingIndexInfo> getIndexInfo() {
        return indexInfo;
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (DanglingIndexInfo info : this.indexInfo) {
            info.toXContent(builder, params);
        }
        return builder.endArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(this.indexInfo.size());
        for (DanglingIndexInfo info : this.indexInfo) {
            info.writeTo(out);
        }
    }
}
