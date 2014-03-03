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
 * A helper to execute explicit merges of the {@link org.apache.lucene.index.IndexWriter} APIs. It
 * holds additional logic which in case the merge scheduler is an {@link org.elasticsearch.index.merge.EnableMergeScheduler}
 * then merges are explicitly enabled and disabled back at the end.
 * <p/>
 * In our codebase, at least until we can somehow use this logic in Lucene IW itself, we should only use
 * this class to execute explicit merges. The explicit merge calls have been added to the forbidden APIs
 * list to make sure we don't call them unless we use this class.
 */
public class Merges {

    /**
     * See {@link org.apache.lucene.index.IndexWriter#maybeMerge()}, with the additional
     * logic of explicitly enabling merges if the scheduler is {@link org.elasticsearch.index.merge.EnableMergeScheduler}.
     */
    public static void maybeMerge(IndexWriter writer) throws IOException {
        MergeScheduler mergeScheduler = writer.getConfig().getMergeScheduler();
        if (mergeScheduler instanceof EnableMergeScheduler) {
            ((EnableMergeScheduler) mergeScheduler).enableMerge();
            try {
                writer.maybeMerge();
            } finally {
                ((EnableMergeScheduler) mergeScheduler).disableMerge();
            }
        } else {
            writer.maybeMerge();
        }
    }

    /**
     * See {@link org.apache.lucene.index.IndexWriter#forceMerge(int)}, with the additional
     * logic of explicitly enabling merges if the scheduler is {@link org.elasticsearch.index.merge.EnableMergeScheduler}.
     */
    public static void forceMerge(IndexWriter writer, int maxNumSegments) throws IOException {
        forceMerge(writer, maxNumSegments, true);
    }

    /**
     * See {@link org.apache.lucene.index.IndexWriter#forceMerge(int, boolean)}, with the additional
     * logic of explicitly enabling merges if the scheduler is {@link org.elasticsearch.index.merge.EnableMergeScheduler}.
     */
    public static void forceMerge(IndexWriter writer, int maxNumSegments, boolean doWait) throws IOException {
        MergeScheduler mergeScheduler = writer.getConfig().getMergeScheduler();
        if (mergeScheduler instanceof EnableMergeScheduler) {
            ((EnableMergeScheduler) mergeScheduler).enableMerge();
            try {
                writer.forceMerge(maxNumSegments, doWait);
            } finally {
                ((EnableMergeScheduler) mergeScheduler).disableMerge();
            }
        } else {
            writer.forceMerge(maxNumSegments, doWait);
        }
    }

    /**
     * See {@link org.apache.lucene.index.IndexWriter#forceMergeDeletes()}, with the additional
     * logic of explicitly enabling merges if the scheduler is {@link org.elasticsearch.index.merge.EnableMergeScheduler}.
     */
    public static void forceMergeDeletes(IndexWriter writer) throws IOException {
        forceMergeDeletes(writer, true);
    }

    /**
     * See {@link org.apache.lucene.index.IndexWriter#forceMergeDeletes(boolean)}, with the additional
     * logic of explicitly enabling merges if the scheduler is {@link org.elasticsearch.index.merge.EnableMergeScheduler}.
     */
    public static void forceMergeDeletes(IndexWriter writer, boolean doWait) throws IOException {
        MergeScheduler mergeScheduler = writer.getConfig().getMergeScheduler();
        if (mergeScheduler instanceof EnableMergeScheduler) {
            ((EnableMergeScheduler) mergeScheduler).enableMerge();
            try {
                writer.forceMergeDeletes(doWait);
            } finally {
                ((EnableMergeScheduler) mergeScheduler).disableMerge();
            }
        } else {
            writer.forceMergeDeletes(doWait);
        }
    }
}
