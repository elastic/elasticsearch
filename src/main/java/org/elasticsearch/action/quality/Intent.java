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

import java.util.Map;

/**
 * Objects of this class represent one type of user query to qa. Each query comprises a user supplied id for easer referencing,
 * a set of parameters as supplied by the end user to the search application as well as a set of rated documents (ratings e.g.
 * supplied by manual result tagging or some form of automated click log based process).
 * 
 * Note: Implements toString for easier debugging, doesn't override equals/hashcode though.
 * */
public class Intent {
    
    private int intentId;
    private Map<String, String> intentParameters;
    private Map<String, Integer> ratedDocuments;

    /** For easier referencing users are allowed to supply unique ids with each search intent they want to check for
     * performance quality wise.*/
    public int getIntentId() {
        return intentId;
    }

    /**
     * @param intentId id used to later reference this query intent.
     * */
    public void setIntentId(int intentId) {
        this.intentId = intentId;
    }
    
    /**
     * Returns a mapping from query parameter name to real parameter - ideally as parsed from real user logs.
     * */
    public Map<String, String> getIntentParameters() {
        return intentParameters;
    }

    /** @param intentParameters Query parameters - typically as supplied by the end user - that are to be substituted into the search
     * request template given in the search {@link Specification}. */
    public void setIntentParameters(Map<String, String> intentParameters) {
        this.intentParameters = intentParameters;
    }

    /**
     * Returns a set of documents and their ratings as supplied by the users.
     * */
    public Map<String, Integer> getRatedDocuments() {
        return ratedDocuments;
    }

    /** @param ratedDocuments Set of documents as expected for this search intent. Each document id is annotated as being either relevant or
     * irrelevant. In the final precision at computation only those documents returned up to a certain position are
     * taken into consideration. Documents of unknown quality (read: haven't been annotated) are returned as is and
     * ignored during precision at n computation. */
    public void setRatedDocuments(Map<String, Integer> ratedDocuments) {
        this.ratedDocuments = ratedDocuments;
    }

    @Override
    public String toString() {
        Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator(":");
        ToStringHelper help = MoreObjects.toStringHelper(this).add("IntentId", intentId);
        
        help.add("Intent parameters", mapJoiner.join(intentParameters));
        help.add("Rated documents", mapJoiner.join(ratedDocuments));
        return help.toString();
    }
}
