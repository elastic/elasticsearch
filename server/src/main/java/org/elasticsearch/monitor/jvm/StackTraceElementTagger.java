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
            ste -> ste.getClassName().endsWith(".DefaultIndexingChain") && ste.getMethodName().equals("flush") ? "FLUSH_SEGMENT" : null,
            ste -> ste.getClassName().endsWith(".IndexWriter") && ste.getMethodName().startsWith("addDocument") ? "APPEND_DOC" : null,
            ste -> ste.getClassName().endsWith(".IndexWriter") && ste.getMethodName().startsWith("updateDocument") ?
                    "APPEND_OR_UPDATE_DOC" : null,
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
