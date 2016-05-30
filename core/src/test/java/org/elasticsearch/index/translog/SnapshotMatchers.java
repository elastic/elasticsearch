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

import org.elasticsearch.ElasticsearchException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.ArrayList;


public final class SnapshotMatchers {
    private SnapshotMatchers() {

    }

    /**
     * Consumes a snapshot and make sure it's size is as expected
     */
    public static Matcher<Translog.Snapshot> size(int size) {
        return new SizeMatcher(size);
    }

    /**
     * Consumes a snapshot and make sure it's content is as expected
     */
    public static Matcher<Translog.Snapshot> equalsTo(Translog.Operation... ops) {
        return new EqualMatcher(ops);
    }

    /**
     * Consumes a snapshot and make sure it's content is as expected
     */
    public static Matcher<Translog.Snapshot> equalsTo(ArrayList<Translog.Operation> ops) {
        return new EqualMatcher(ops.toArray(new Translog.Operation[ops.size()]));
    }

    public static class SizeMatcher extends TypeSafeMatcher<Translog.Snapshot> {

        private final int size;

        public SizeMatcher(int size) {
            this.size = size;
        }

        @Override
        public boolean matchesSafely(Translog.Snapshot snapshot) {
            int count = 0;
            try {
                while (snapshot.next() != null) {
                    count++;
                }
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to advance snapshot", ex);
            }
            return size == count;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a snapshot with size ").appendValue(size);
        }
    }

    public static class EqualMatcher extends TypeSafeMatcher<Translog.Snapshot> {

        private final Translog.Operation[] expectedOps;
        String failureMsg = null;

        public EqualMatcher(Translog.Operation[] expectedOps) {
            this.expectedOps = expectedOps;
        }

        @Override
        protected boolean matchesSafely(Translog.Snapshot snapshot) {
            try {
                Translog.Operation op;
                int i;
                for (i = 0, op = snapshot.next(); op != null && i < expectedOps.length; i++, op = snapshot.next()) {
                    if (expectedOps[i].equals(op) == false) {
                        failureMsg = "position [" + i + "] expected [" + expectedOps[i] + "] but found [" + op + "]";
                        return false;
                    }
                }

                if (i < expectedOps.length) {
                    failureMsg = "expected [" + expectedOps.length + "] ops but only found [" + i + "]";
                    return false;
                }

                if (op != null) {
                    int count = 1; // to account for the op we already read
                    while (snapshot.next() != null) {
                        count++;
                    }
                    failureMsg = "expected [" + expectedOps.length + "] ops but got [" + (expectedOps.length + count) + "]";
                    return false;
                }
                return true;
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to read snapshot content", ex);
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(failureMsg);
        }
    }


}
