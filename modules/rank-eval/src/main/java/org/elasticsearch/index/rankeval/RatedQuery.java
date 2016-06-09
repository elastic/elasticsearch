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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Objects of this class represent one type of user query to qa. Each query comprises a user supplied id for easer referencing,
 * a set of parameters as supplied by the end user to the search application as well as a set of rated documents (ratings e.g.
 * supplied by manual result tagging or some form of automated click log based process).
 * */
public class RatedQuery implements Writeable {
    
    private final int intentId;
    private final Map<String, Object> intentParameters;
    private final Map<String, Integer> ratedDocuments;

    public RatedQuery(
            int intentId, Map<String, Object> intentParameters, Map<String, Integer> ratedDocuments) {
        this.intentId = intentId;
        this.intentParameters = intentParameters;
        this.ratedDocuments = ratedDocuments;
    }

    public RatedQuery(StreamInput in) throws IOException {
        this.intentId = in.readInt();
        this.intentParameters = in.readMap();
        
        int ratedDocsSize = in.readInt();
        this.ratedDocuments = new HashMap<>(ratedDocsSize);
        for (int i = 0; i < ratedDocsSize; i++) {
            this.ratedDocuments.put(in.readString(), in.readInt());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(intentId);
        out.writeMap(intentParameters);
        out.writeInt(ratedDocuments.size());
        for(Entry<String, Integer> entry : ratedDocuments.entrySet()) {
            out.writeString(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }
    
    /** For easier referencing users are allowed to supply unique ids with each search intent they want to check for
     * performance quality wise.*/
    public int getIntentId() {
        return intentId;
    }

    
    /**
     * Returns a mapping from query parameter name to real parameter - ideally as parsed from real user logs.
     * */
    public Map<String, Object> getIntentParameters() {
        return intentParameters;
    }

    /**
     * Returns a set of documents and their ratings as supplied by the users.
     * */
    public Map<String, Integer> getRatedDocuments() {
        return ratedDocuments;
    }

}
