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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.Strings.EMPTY_ARRAY;

public class RestoreDanglingIndicesRequest extends BaseNodesRequest<RestoreDanglingIndicesRequest> {
    private String[] indexIds = EMPTY_ARRAY;

    public RestoreDanglingIndicesRequest(StreamInput in) throws IOException {
        super(in);
    }

    public RestoreDanglingIndicesRequest() {
        super(new String[0]);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (this.indexIds.length == 0) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("No index IDs specified");
            return e;
        }

        return null;
    }

    public String[] getIndexIds() {
        return indexIds;
    }

    public void setIndexIds(String[] indexIds) {
        this.indexIds = indexIds;
    }

    @Override
    public String toString() {
        return "restore dangling indices";
    }

    @SuppressWarnings("unchecked")
    public void source(Map<String, Object> source) {
        source.forEach((name, value) -> {
            if (name.equals("ids")) {
                if (value instanceof List) {
                    this.indexIds = ((List<String>) value).toArray(new String[0]);
                } else {
                    throw new IllegalArgumentException("malformed indices section, should be an array of strings");
                }
            }

            throw new IllegalArgumentException("unknown key [" + name + "]");
        });
    }
}
