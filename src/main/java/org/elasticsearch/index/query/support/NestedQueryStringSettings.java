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

package org.elasticsearch.index.query.support;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.internal.SubSearchContext;

public class NestedQueryStringSettings {
    private String path;
    private ScoreMode scoreMode;
    private Tuple<String, SubSearchContext> innerHits;
    
    public String getPath() {
        return path;
    }
    
    public void setPath(String path) {
        this.path = path;
    }
    
    public ScoreMode getScoreMode() {
        return scoreMode;
    }
    
    public void setScoreMode(ScoreMode scoreMode) {
        this.scoreMode = scoreMode;
    }
    
    public Tuple<String, SubSearchContext> getInnerHits() {
        return innerHits;
    }
    
    public void setInnerHits(Tuple<String, SubSearchContext> innerHits) {
        this.innerHits = innerHits;
    }
}