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

import java.util.List;

public interface TranslogDeletionPolicy {

    /** called when a new translog generation is created */
    void onTranslogRollover(List<TranslogReader> readers, TranslogWriter currentWriter);

    /**
     * acquires the basis generation for a new view. Any translog generation above, and including, the returned generation
     * will not be deleted until a corresponding call to {@link #releaseTranslogGenView(long)} is called.
     */
    long acquireTranslogGenForView();

    /** returns the number of generations that were acquired for views */
    int pendingViewsCount();

    /** releases a generation that was acquired by calling {@link #acquireTranslogGenForView()} */
    void releaseTranslogGenView(long translogGen);

    /**
     * returns the minimum translog generation that is still required by the system. Any generation below
     * the returned value may be safely deleted
     */
    long minTranslogGenRequired(List<TranslogReader> readers, TranslogWriter currentWriter);

    /** returns the translog generation that will be used as a basis of a future store/peer recovery */
    long getMinTranslogGenerationForRecovery();
}
