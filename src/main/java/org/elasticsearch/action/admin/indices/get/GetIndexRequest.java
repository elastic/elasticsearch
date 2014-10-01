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

package org.elasticsearch.action.admin.indices.get;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.info.ClusterInfoRequest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to delete an index. Best created with {@link org.elasticsearch.client.Requests#deleteIndexRequest(String)}.
 */
public class GetIndexRequest extends ClusterInfoRequest<GetIndexRequest> {

    private String[] features = new String[] { "_settings", "_warmers", "_mappings", "_aliases" };
    private boolean indicesOptionsSet = false;

    public GetIndexRequest features(String[] features) {
        if (features == null) {
            throw new ElasticsearchIllegalArgumentException("features cannot be null");
        } else {
            this.features = features;
        }
        return this;
    }

    public String[] features() {
        return features;
    }
    
    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        features = in.readStringArray();
        indicesOptionsSet = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(features);
        out.writeBoolean(indicesOptionsSet);
    }

    @Override
    public GetIndexRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptionsSet = true;
        return super.indicesOptions(indicesOptions);
    }

    @Override
    public IndicesOptions indicesOptions() {
        if (!indicesOptionsSet) {
            indicesOptions(resolveIndicesOptions());
        }
        IndicesOptions indicesOptions = super.indicesOptions();
        return indicesOptions;
    }

    private IndicesOptions resolveIndicesOptions() {
        IndicesOptions defaultIndicesOptions = IndicesOptions.strictExpandOpen();
        String[] indices = indices();
        // This makes sure that the get aliases API behaves exactly like in previous versions wrt indices options iff only aliases are requested
        if (features != null && features.length == 1 && features[0] != null && ("_alias".equals(features[0]) || "_aliases".equals(features[0]))) {
            // If we are asking for all indices we need to return open and closed, if not we only expand to open
            if (MetaData.isAllIndices(indices)) {
                defaultIndicesOptions = IndicesOptions.fromOptions(true, true, true, true);
            } else {
                defaultIndicesOptions = IndicesOptions.lenientExpandOpen();
            }
        }
        return defaultIndicesOptions;
    }

}
