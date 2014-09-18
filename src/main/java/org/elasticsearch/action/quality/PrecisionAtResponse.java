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

package org.elasticsearch.action.quality;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

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
public class PrecisionAtResponse extends ActionResponse {

    private Collection<PrecisionResult> precisionResults = new ArrayList<PrecisionResult>();
    
    public void addPrecisionAt(int specId, double precision, Map<Integer, Collection<String>> unknownDocs) {
        PrecisionResult result = new PrecisionResult();
        result.setSpecId(specId);
        result.setPrecision(precision);
        result.setUnknownDocs(unknownDocs);
        this.precisionResults.add(result);
    }
    
    public Collection<PrecisionResult> getPrecision() {
        return precisionResults;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(precisionResults.size());
        for (PrecisionResult result : precisionResults) {
            out.writeInt(result.specId);
            out.writeDouble(result.precision);
            out.writeGenericValue(result.getUnknownDocs());
        }
    }    
    
    @Override
    @SuppressWarnings("unchecked")
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        precisionResults = new ArrayList<PrecisionResult>();
        int resultSize = in.readInt();
        for (int i = 0; i < resultSize; i++) {
            PrecisionResult result = new PrecisionResult();
            result.setSpecId(in.readInt());
            result.setPrecision(in.readDouble());
            result.setUnknownDocs((Map<Integer, Collection<String>>) in.readGenericValue()); 
            precisionResults.add(result);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("PrecisionResults", Joiner.on(':').join(precisionResults)).toString();
    }
    
    /**
     * For each precision at n computation the id of the search request specification used to generate search requests is returned
     * for reference. In addition the averaged precision and the ids of all documents returned but not found annotated is returned.
     * */
    public class PrecisionResult {
        /**ID of specification this result was generated for.*/
        private int specId;
        /**Average precision observed when issueing query intents with this spec.*/
        private double precision;
        /**Mapping from intent id to all documents seen for this intent that were not annotated.*/
        private Map<Integer, Collection<String>> unknownDocs;
        
        public int getSpecId() {
            return specId;
        }
        public void setSpecId(int specId) {
            this.specId = specId;
        }
        public double getPrecision() {
            return precision;
        }
        public void setPrecision(double precision) {
            this.precision = precision;
        }
        public Map<Integer, Collection<String>> getUnknownDocs() {
            return unknownDocs;
        }
        public void setUnknownDocs(Map<Integer, Collection<String>> unknownDocs) {
            this.unknownDocs = unknownDocs;
        }
        
        @Override
        public String toString() {
            ToStringHelper help = MoreObjects.toStringHelper(this).add("Spec id", specId);
            help.add("Precision", precision);
            StringBuffer unknown = new StringBuffer(); 
            for (Entry<Integer, Collection<String>> unknownDoc : unknownDocs.entrySet()) {
                unknown.append(unknownDoc.getKey());
                unknown.append(":");
                unknown.append(Joiner.on(',').join(unknownDoc.getValue()));
                unknown.append(";");
            }
            help.add("Unknown docs", unknown);
            return help.toString();
        }
    }
}
