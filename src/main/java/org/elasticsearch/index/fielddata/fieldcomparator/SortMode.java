/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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


package org.elasticsearch.index.fielddata.fieldcomparator;

import org.elasticsearch.ElasticSearchIllegalArgumentException;

/**
 * Defines what values to pick in the case a document contains multiple values for a particular field.
 */
public enum SortMode {

    /**
     * Sum of all the values.
     */
    SUM,

    /**
     * Average of all the values.
     */
    AVG,

    /**
     * Pick the lowest value.
     */
    MIN,

    /**
     * Pick the highest value.
     */
    MAX;

    public static SortMode fromString(String sortMode) {
        if ("min".equals(sortMode)) {
            return MIN;
        } else if ("max".equals(sortMode)) {
            return MAX;
        } else if ("sum".equals(sortMode)) {
            return SUM;
        } else if ("avg".equals(sortMode)) {
            return AVG;
        } else {
            throw new ElasticSearchIllegalArgumentException("Illegal sort_mode " + sortMode);
        }
    }

}
