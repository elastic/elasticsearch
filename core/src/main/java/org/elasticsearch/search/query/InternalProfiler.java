package org.elasticsearch.search.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.profile.InternalProfileResults;
import org.elasticsearch.search.profile.TimingWrapper;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;


public class InternalProfiler {

    private Map<Query, TimingWrapper> timings;
    private Map<Query, ArrayList<Query>> tree;
    private Deque<Query> stack;
    private Query root;

    public InternalProfiler() {
        timings = new HashMap<>(10);
        stack = new LinkedBlockingDeque<>(10);
        tree = new HashMap<>(10);
    }

    public void startTime(Query query, TimingWrapper.TimingType timing) {
        TimingWrapper queryTimings = timings.get(query);

        if (queryTimings == null) {
            queryTimings = new TimingWrapper();
        }

        queryTimings.startTime(timing);
        timings.put(query, queryTimings);

    }

    public void stopAndRecordTime(Query query, TimingWrapper.TimingType timing) {
        TimingWrapper queryTimings = timings.get(query);
        queryTimings.stopAndRecordTime(timing);
        timings.put(query, queryTimings);

    }

    public void reconcileRewrite(Query original, Query rewritten) {
        TimingWrapper originalTimings = timings.get(original);

        TimingWrapper rewrittenTimings = timings.get(rewritten);
        if (rewrittenTimings == null) {
            rewrittenTimings = new TimingWrapper();
        }
        rewrittenTimings.setTime(TimingWrapper.TimingType.REWRITE, originalTimings.getTime(TimingWrapper.TimingType.REWRITE));
        timings.put(rewritten, rewrittenTimings);
        timings.remove(original);
    }

    public void pushQuery(Query query) {
        if (stack.size() != 0) {
            updateParent(query);
        } else {
            root = query;
        }

        addNode(query);
        stack.add(query);
    }

    public void pollLast() {
        stack.pollLast();
    }

    public InternalProfileResults finalizeProfileResults() {
        return doFinalizeProfileResults(root);
    }

    private InternalProfileResults doFinalizeProfileResults(Query query) {
        InternalProfileResults rootNode =  new InternalProfileResults(query, timings.get(query));
        ArrayList<Query> children = tree.get(query);

        for (Query child : children) {
            InternalProfileResults childNode = doFinalizeProfileResults(child);
            rootNode.addChild(childNode);
        }

        return rootNode;
    }

    private void addNode(Query query) {
        tree.put(query, new ArrayList<Query>(5));
    }

    private void updateParent(Query child) {
        Query parent = stack.peekLast();
        ArrayList<Query> parentNode = tree.get(parent);
        parentNode.add(child);
        tree.put(parent, parentNode);
    }


}
