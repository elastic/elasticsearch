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

package org.elasticsearch.index.translog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TranslogDeletionPolicy implements DeletionPolicy {

    /** Records how many views are held against each
     *  translog generation */
    protected final Map<Long,Integer> translogRefCounts = new HashMap<>();

    /**
     * the translog generation that is requires to properly recover from the oldest non deleted
     * {@link org.apache.lucene.index.IndexCommit}.
     */
    private long minTranslogGenerationForRecovery = -1;

    @Override
    public void onTranslogRollover(List<TranslogReader> readers, TranslogWriter currentWriter) {

    }

    @Override
    public long acquireTranslogGenForView() {
        return 0;
    }

    @Override
    public int pendingViewsCount() {
        return 0;
    }

    @Override
    public void releaseTranslogGenView(long translogGen) {

    }

    @Override
    public long minTranslogGenRequired(List<TranslogReader> readers, TranslogWriter currentWriter) {
        return 0;
    }

    @Override
    public long getMinTranslogGenerationForRecovery() {
        return 0;
    }
}
