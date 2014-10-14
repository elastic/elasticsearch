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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

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
public class QualityResponse extends ActionResponse {

    private Collection<QualityResult> qualityResults = new ArrayList<QualityResult>();
    
    public void addQualityResult(int specId, double quality, Map<Integer, Collection<String>> unknownDocs) {
        QualityResult result = new QualityResult();
        result.setSpecId(specId);
        result.setQualityLevel(quality);
        result.setUnknownDocs(unknownDocs);
        this.qualityResults.add(result);
    }
    
    public Collection<QualityResult> getPrecision() {
        return qualityResults;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(qualityResults.size());
        for (QualityResult result : qualityResults) {
            result.writeTo(out);
        }
    }    
    
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        qualityResults = new ArrayList<QualityResult>();
        int resultSize = in.readInt();
        for (int i = 0; i < resultSize; i++) {
            QualityResult result = new QualityResult();
            result.readFrom(in);
            qualityResults.add(result);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("QualityResults", Joiner.on(':').join(qualityResults)).toString();
    }
}
