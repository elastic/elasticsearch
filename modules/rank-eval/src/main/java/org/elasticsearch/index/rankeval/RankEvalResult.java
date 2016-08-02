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
import java.util.Collection;
import java.util.Map;

/**
 * For each precision at n computation the id of the search request specification used to generate search requests is returned
 * for reference. In addition the averaged precision and the ids of all documents returned but not found annotated is returned.
 * */
// TODO do we need an extra class for this or it RankEvalResponse enough?
// TODO instead of just returning averages over complete results, think of other statistics, micro avg, macro avg, partial results
public class RankEvalResult implements Writeable {
    /**ID of QA specification this result was generated for.*/
    private String specId;
    /**Average precision observed when issuing query intents with this specification.*/
    private double qualityLevel;
    /**Mapping from intent id to all documents seen for this intent that were not annotated.*/
    private Map<String, Collection<String>> unknownDocs;

    @SuppressWarnings("unchecked")
    public RankEvalResult(StreamInput in) throws IOException {
        this.specId = in.readString();
        this.qualityLevel = in.readDouble();
        this.unknownDocs = (Map<String, Collection<String>>) in.readGenericValue();
    }

    public RankEvalResult(String specId, double quality, Map<String, Collection<String>> unknownDocs) {
        this.specId = specId;
        this.qualityLevel = quality;
        this.unknownDocs = unknownDocs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(specId);
        out.writeDouble(qualityLevel);
        out.writeGenericValue(getUnknownDocs());
    }

    public String getSpecId() {
        return specId;
    }

    public double getQualityLevel() {
        return qualityLevel;
    }

    public Map<String, Collection<String>> getUnknownDocs() {
        return unknownDocs;
    }

    @Override
    public String toString() {
        return "RankEvalResult, ID :[" + specId + "], quality: " + qualityLevel + ", unknown docs: " + unknownDocs;
    }
}