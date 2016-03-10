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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.reindex.BulkByScrollTask.Status;

import java.io.IOException;
import java.util.List;

/**
 * Response for the ReindexAction.
 */
public class ReindexResponse extends BulkIndexByScrollResponse {
    public ReindexResponse() {
    }

    public ReindexResponse(TimeValue took, Status status, List<Failure> indexingFailures, List<ShardSearchFailure> searchFailures) {
        super(took, status, indexingFailures, searchFailures);
    }

    public long getCreated() {
        return getStatus().getCreated();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("took", getTook());
        getStatus().innerXContent(builder, params, true, false);
        builder.startArray("failures");
        for (Failure failure: getIndexingFailures()) {
            builder.startObject();
            failure.toXContent(builder, params);
            builder.endObject();
        }
        for (ShardSearchFailure failure: getSearchFailures()) {
            builder.startObject();
            failure.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ReindexResponse[");
        builder.append("took=").append(getTook()).append(',');
        getStatus().innerToString(builder, true, false);
        return builder.append(']').toString();
    }
}
