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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/** 
 * For each qa specification identified by its id this response returns the respective
 * averaged precisionAnN value.
 * 
 * TODO is this a micro-/ or macro average?
 **/
public class PrecisionAtResponse extends ActionResponse {

    private Collection<PrecisionResult> precisionResults;
    
    public void addPrecisionAt(int specId, double precision, Collection<String> unknownDocs) {
        PrecisionResult result = new PrecisionResult();
        result.setSpecId(specId);
        result.setPrecision(precision);
        result.setUnknownDocs(unknownDocs);
        this.precisionResults.add(result);
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
            result.setUnknownDocs((Collection<String>) in.readGenericValue()); 
            precisionResults.add(result);
        }
    }
    
    public class PrecisionResult {
        private int specId;
        private double precision;
        private Collection<String> unknownDocs;
        
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
        public Collection<String> getUnknownDocs() {
            return unknownDocs;
        }
        public void setUnknownDocs(Collection<String> unknownDocs) {
            this.unknownDocs = unknownDocs;
        }
    }
}
