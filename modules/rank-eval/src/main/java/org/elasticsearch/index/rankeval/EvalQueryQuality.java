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

import java.util.Collection;

/** Returned for each search intent and search specification combination. Summarises the document ids found that were not
 * annotated and the average precision of result sets in each particular combination based on the annotations given. 
 * */
public class EvalQueryQuality {
    private double qualityLevel;
        
    private Collection<String> unknownDocs;

    public EvalQueryQuality (double qualityLevel, Collection<String> unknownDocs) {
       this.qualityLevel = qualityLevel;
       this.unknownDocs = unknownDocs;
    }
        
    public Collection<String> getUnknownDocs() {
        return unknownDocs;
    }

    public double getQualityLevel() {
        return qualityLevel;          
    }

}
