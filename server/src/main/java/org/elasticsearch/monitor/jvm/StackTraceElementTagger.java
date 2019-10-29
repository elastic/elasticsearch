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

package org.elasticsearch.monitor.jvm;

import java.util.List;
import java.util.function.Function;

/**
 * Associate a {@link StackTraceElement} with a tag that helps identify what the stack trace is about.
 */
final class StackTraceElementTagger {

    private StackTraceElementTagger() {}

    private static final List<Function<StackTraceElement, String>> TAGGERS = List.of(
            ste -> ste.getMethodName().equals("incrementToken") ? "ANALYSIS" : null,
            ste -> ste.getClassName().endsWith(".SegmentMerger") && ste.getMethodName().equals("merge") ? "MERGE_SEGMENT" : null,
            ste -> ste.getClassName().endsWith(".DocumentsWriterPerThread") && ste.getMethodName().equals("flush") ?
                    "CREATE_SEGMENT" : null, // avoid using "flush" in the name, which has different meanings in Lucene and ES
            ste -> ste.getClassName().endsWith(".IndexWriter") && ste.getMethodName().startsWith("addDocument") ? "APPEND_DOC" : null,
            ste -> ste.getClassName().endsWith(".IndexWriter") && ste.getMethodName().startsWith("updateDocument") ?
                    "APPEND_OR_OVERWRITE_DOC" : null, // use "overwrite" instead of "update" to avoid confusion with the _update API 
            ste -> ste.getClassName().endsWith(".IndexWriter") && ste.getMethodName().equals("getReader") ? "REFRESH_SHARD" : null,
            ste -> ste.getClassName().endsWith(".ReadersAndUpdates") && ste.getMethodName().equals("writeFieldUpdates") ?
                    "WRITE_SEGMENT_UPDATES" : null
    );

    public static String tag(StackTraceElement element) {
        for (Function<StackTraceElement, String> tagger : TAGGERS) {
            String tag = tagger.apply(element);
            if (tag != null) {
                // First one that matches wins
                return tag;
            }
        }
        return null;
    }

}
