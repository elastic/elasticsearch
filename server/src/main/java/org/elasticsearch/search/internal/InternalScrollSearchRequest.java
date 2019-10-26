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

package org.elasticsearch.search.internal;

import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;

public class InternalScrollSearchRequest extends TransportRequest {

    private long id;

    private Scroll scroll;

    public InternalScrollSearchRequest() {
    }

    public InternalScrollSearchRequest(SearchScrollRequest request, long id) {
        this.id = id;
        this.scroll = request.scroll();
    }

    public InternalScrollSearchRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readLong();
        scroll = in.readOptionalWriteable(Scroll::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(id);
        out.writeOptionalWriteable(scroll);
    }

    public long id() {
        return id;
    }

    public Scroll scroll() {
        return scroll;
    }

    public InternalScrollSearchRequest scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        return "id[" + id + "], scroll[" + scroll + "]";
    }

}
