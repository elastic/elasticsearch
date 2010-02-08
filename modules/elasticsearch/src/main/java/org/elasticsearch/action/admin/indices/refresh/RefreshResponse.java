/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class RefreshResponse implements ActionResponse, Streamable {

    private Map<String, IndexRefreshResponse> indices = new HashMap<String, IndexRefreshResponse>();

    RefreshResponse() {

    }

    public Map<String, IndexRefreshResponse> indices() {
        return indices;
    }

    public IndexRefreshResponse index(String index) {
        return indices.get(index);
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            IndexRefreshResponse response = new IndexRefreshResponse();
            response.readFrom(in);
            indices.put(response.index(), response);
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeInt(indices.size());
        for (IndexRefreshResponse indexRefreshResponse : indices.values()) {
            indexRefreshResponse.writeTo(out);
        }
    }
}
