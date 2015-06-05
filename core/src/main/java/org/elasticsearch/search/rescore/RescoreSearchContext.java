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

package org.elasticsearch.search.rescore;



/**
 */
public class RescoreSearchContext {

    private int windowSize;
    
    private final String type;

    private final Rescorer rescorer;

    public RescoreSearchContext(String type, int windowSize, Rescorer rescorer) {
        super();
        this.type = type;
        this.windowSize = windowSize;
        this.rescorer = rescorer;
    }
    
    public Rescorer rescorer() {
        return rescorer;
    }

    public String getType() {
        return type;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    public int window() {
        return windowSize;
    }
    
}
