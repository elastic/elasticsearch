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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * For each qa specification identified by its id this response returns the respective
 * averaged precisionAnN value.
 *
 * In addition for each query the document ids that haven't been found annotated is returned as well.
 *
 * Documents of unknown quality - i.e. those that haven't been supplied in the set of annotated documents but have been returned
 * by the search are not taken into consideration when computing precision at n - they are ignored.
 *
 **/
//TODO instead of just returning averages over complete results, think of other statistics, micro avg, macro avg, partial results
public class RankEvalResponse extends ActionResponse implements ToXContent {
    /**Average precision observed when issuing query intents with this specification.*/
    private double qualityLevel;
    /**Mapping from intent id to all documents seen for this intent that were not annotated.*/
    private Map<String, Collection<RatedDocumentKey>> unknownDocs;

    public RankEvalResponse() {
    }

    public RankEvalResponse(double qualityLevel, Map<String, Collection<RatedDocumentKey>> unknownDocs) {
        this.qualityLevel = qualityLevel;
        this.unknownDocs = unknownDocs;
    }

    public double getQualityLevel() {
        return qualityLevel;
    }

    public Map<String, Collection<RatedDocumentKey>> getUnknownDocs() {
        return unknownDocs;
    }

    @Override
    public String toString() {
        return "RankEvalResponse, quality: " + qualityLevel + ", unknown docs: " + unknownDocs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeDouble(qualityLevel);
        out.writeVInt(unknownDocs.size());
        for (String queryId : unknownDocs.keySet()) {
            out.writeString(queryId);
            Collection<RatedDocumentKey> collection = unknownDocs.get(queryId);
            out.writeVInt(collection.size());
            for (RatedDocumentKey key : collection) {
                key.writeTo(out);
            }
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.qualityLevel = in.readDouble();
        int unknownDocumentSets = in.readVInt();
        this.unknownDocs = new HashMap<>(unknownDocumentSets);
        for (int i = 0; i < unknownDocumentSets; i++) {
            String queryId = in.readString();
            int numberUnknownDocs = in.readVInt();
            Collection<RatedDocumentKey> collection = new ArrayList<>(numberUnknownDocs);
            for (int d = 0; d < numberUnknownDocs; d++) {
                collection.add(new RatedDocumentKey(in));
            }
            this.unknownDocs.put(queryId, collection);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("rank_eval");
        builder.field("quality_level", qualityLevel);
        builder.startObject("unknown_docs");
        for (String key : unknownDocs.keySet()) {
            Collection<RatedDocumentKey> keys = unknownDocs.get(key);
            builder.startArray(key);
            for (RatedDocumentKey docKey : keys) {
                builder.startObject();
                builder.field(RatedDocument.INDEX_FIELD.getPreferredName(), docKey.getIndex());
                builder.field(RatedDocument.TYPE_FIELD.getPreferredName(), docKey.getType());
                builder.field(RatedDocument.DOC_ID_FIELD.getPreferredName(), docKey.getDocID());
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RankEvalResponse other = (RankEvalResponse) obj;
        return Objects.equals(qualityLevel, other.qualityLevel) &&
                Objects.equals(unknownDocs, other.unknownDocs);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), qualityLevel, unknownDocs);
    }
}
