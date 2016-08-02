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

package org.elasticsearch.search.fetch;

import org.elasticsearch.search.fetch.docvalues.DocValueFieldsContext;

/**
 * All configuration and context needed by the FetchSubPhase to execute on hits.
 * The only required information in this base class is whether or not the sub phase needs to be run at all.
 * It can be extended by FetchSubPhases to hold information the phase needs to execute on hits.
 * See {@link org.elasticsearch.search.fetch.FetchSubPhase.ContextFactory} and also {@link DocValueFieldsContext} for an example.
 */
public class FetchSubPhaseContext {

    // This is to store if the FetchSubPhase should be executed at all.
    private boolean hitExecutionNeeded = false;

    /**
     * Set if this phase should be executed at all.
     */
    public void setHitExecutionNeeded(boolean hitExecutionNeeded) {
        this.hitExecutionNeeded = hitExecutionNeeded;
    }

    /**
     * Returns if this phase be executed at all.
     */
    public boolean hitExecutionNeeded() {
        return hitExecutionNeeded;
    }

}
