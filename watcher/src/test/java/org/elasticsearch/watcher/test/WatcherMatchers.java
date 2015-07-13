/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test;

import org.elasticsearch.action.index.IndexRequest;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.mockito.Matchers;

/**
 *
 */
public final class WatcherMatchers {

    private WatcherMatchers() {
    }

    public static IndexRequest indexRequest(String index, String type, String id) {
        return Matchers.argThat(indexRequestMatcher(index, type, id));
    }

    public static IndexRequest indexRequest(String index, String type, String id, IndexRequest.OpType opType) {
        return Matchers.argThat(indexRequestMatcher(index, type, id).opType(opType));
    }

    public static IndexRequest indexRequest(String index, String type, String id, Long version, IndexRequest.OpType opType) {
        return Matchers.argThat(indexRequestMatcher(index, type, id).version(version).opType(opType));
    }

    public static IndexRequestMatcher indexRequestMatcher(String index, String type, String id) {
        return new IndexRequestMatcher(index, type, id);
    }

    public static class IndexRequestMatcher extends TypeSafeMatcher<IndexRequest> {

        private final String index;
        private final String type;
        private final String id;
        private Long version;
        private IndexRequest.OpType opType;

        private IndexRequestMatcher(String index, String type, String id) {
            this.index = index;
            this.type = type;
            this.id = id;
        }

        public IndexRequestMatcher version(long version) {
            this.version = version;
            return this;
        }

        public IndexRequestMatcher opType(IndexRequest.OpType opType) {
            this.opType = opType;
            return this;
        }

        @Override
        protected boolean matchesSafely(IndexRequest request) {
            if (!index.equals(request.index()) || !type.equals(request.type()) || !id.equals(request.id())) {
                return false;
            }
            if (version != null && !version.equals(request.version())) {
                return false;
            }
            if (opType != null && !opType.equals(request.opType())) {
                return false;
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("is index request [" + index + "/" + type + "/" + id + "]");
        }
    }
}
