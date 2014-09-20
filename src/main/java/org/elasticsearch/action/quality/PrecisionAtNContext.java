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

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Objects of this class encode the level of search results to take into consideration when computing precision.
 * 
 * This should have some correlation to the user interface in production showing search results to users - i.e. if a user sees ten
 * search results per result page this should be set to 10.
 * */
public class PrecisionAtNContext implements QualityContext {

    private PrecisionAtN precisionMetric;

    public void setN(int n) {
        this.precisionMetric = new PrecisionAtN(n);
    }
    
    @Override
    public String toString() {
        ToStringHelper help = MoreObjects.toStringHelper(this).add("n", precisionMetric.getN());
        return help.toString();
    }

    @Override
    public RankedListQualityMetric getMetric() {
        return precisionMetric;
    }

    @Override
    public void write(StreamOutput out) throws IOException {
        out.writeInt(precisionMetric.getN());
    }

    @Override
    public void read(StreamInput in) throws IOException {
        this.precisionMetric = new PrecisionAtN(in.readInt());
    }
}
