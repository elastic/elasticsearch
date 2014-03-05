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

package org.elasticsearch.index.merge;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;

import java.io.IOException;

/**
 * A wrapper of another {@link org.apache.lucene.index.MergeScheduler} that allows
 * to explicitly enable merge and disable on a thread local basis. The default is
 * to have merges disabled.
 * <p/>
 * This merge scheduler can be used to get around the fact that even though a merge
 * policy can control that no new merges will be created as a result of a segment flush
 * (during indexing operation for example), the {@link #merge(org.apache.lucene.index.IndexWriter)}
 * call will still be called, and can result in stalling indexing.
 */
public class EnableMergeScheduler extends MergeScheduler {

    private final MergeScheduler mergeScheduler;

    private final ThreadLocal<Boolean> enabled = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return Boolean.FALSE;
        }
    };

    public EnableMergeScheduler(MergeScheduler mergeScheduler) {
        this.mergeScheduler = mergeScheduler;
    }

    /**
     * Enable merges on the current thread.
     */
    void enableMerge() {
        assert !enabled.get();
        enabled.set(Boolean.TRUE);
    }

    /**
     * Disable merges on the current thread.
     */
    void disableMerge() {
        assert enabled.get();
        enabled.set(Boolean.FALSE);
    }

    @Override
    public void merge(IndexWriter writer) throws IOException {
        if (enabled.get()) {
            mergeScheduler.merge(writer);
        }
    }

    @Override
    public void close() throws IOException {
        mergeScheduler.close();
    }

    @Override
    public MergeScheduler clone() {
        // Lucene IW makes a clone internally but since we hold on to this instance
        // the clone will just be the identity.
        return this;
    }
}
