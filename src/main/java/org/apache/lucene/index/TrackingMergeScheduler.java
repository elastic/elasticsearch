package org.apache.lucene.index;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.concurrent.ConcurrentMap;

/**
 */
public class TrackingMergeScheduler {

    private static final ConcurrentMap<Thread, MergePolicy.OneMerge> merges = ConcurrentCollections.newConcurrentMap();

    public static void setCurrentMerge(MergePolicy.OneMerge merge) {
        merges.put(Thread.currentThread(), merge);
    }

    public static void removeCurrentMerge() {
        merges.remove(Thread.currentThread());
    }

    public static MergePolicy.OneMerge getCurrentMerge() {
        return merges.get(Thread.currentThread());
    }
}
