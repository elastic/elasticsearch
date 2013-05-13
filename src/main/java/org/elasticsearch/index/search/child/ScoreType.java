/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.search.child;

import org.elasticsearch.ElasticSearchIllegalArgumentException;

/**
 * Defines how scores from child documents are mapped into the parent document.
 */
public enum ScoreType {

    /**
     * Only the highest score of all matching child documents is mapped into the parent.
     */
    MAX,

    /**
     * The average score based on all matching child documents are mapped into the parent.
     */
    AVG,

    /**
     * The matching children scores is summed up and mapped into the parent.
     */
    SUM;

    public static ScoreType fromString(String type) {
        if ("max".equals(type)) {
            return MAX;
        } else if ("avg".equals(type)) {
            return AVG;
        } else if ("sum".equals(type)) {
            return SUM;
        } else if ("total".equals(type)) { // This name is consistent with: ScoreMode.Total
            return SUM;
        }
        throw new ElasticSearchIllegalArgumentException("No score type for child query [" + type + "] found");
    }

}
