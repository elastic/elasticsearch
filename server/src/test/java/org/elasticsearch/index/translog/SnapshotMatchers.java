/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.exception.ElasticsearchException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    public static Matcher<Translog.Snapshot> equalsTo(List<Translog.Operation> ops) {
        return new EqualMatcher(ops.toArray(new Translog.Operation[ops.size()]));
    }

    public static Matcher<Translog.Snapshot> containsOperationsInAnyOrder(Collection<Translog.Operation> expectedOperations) {
        return new ContainingInAnyOrderMatcher(expectedOperations);
    }

    /**
     * Consumes a snapshot and makes sure that its operations have all seqno between minSeqNo(inclusive) and maxSeqNo(inclusive).
     */
    public static Matcher<Translog.Snapshot> containsSeqNoRange(long minSeqNo, long maxSeqNo) {
        return new ContainingSeqNoRangeMatcher(minSeqNo, maxSeqNo);
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

    public static class ContainingInAnyOrderMatcher extends TypeSafeMatcher<Translog.Snapshot> {
        private final Collection<Translog.Operation> expectedOps;
        private List<Translog.Operation> notFoundOps;
        private List<Translog.Operation> notExpectedOps;

        static List<Translog.Operation> drainAll(Translog.Snapshot snapshot) throws IOException {
            final List<Translog.Operation> actualOps = new ArrayList<>();
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                actualOps.add(op);
            }
            return actualOps;
        }

        public ContainingInAnyOrderMatcher(Collection<Translog.Operation> expectedOps) {
            this.expectedOps = expectedOps;
        }

        @Override
        protected boolean matchesSafely(Translog.Snapshot snapshot) {
            try {
                List<Translog.Operation> actualOps = drainAll(snapshot);
                notFoundOps = expectedOps.stream().filter(o -> actualOps.contains(o) == false).toList();
                notExpectedOps = actualOps.stream().filter(o -> expectedOps.contains(o) == false).toList();
                return notFoundOps.isEmpty() && notExpectedOps.isEmpty();
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to read snapshot content", ex);
            }
        }

        @Override
        protected void describeMismatchSafely(Translog.Snapshot snapshot, Description mismatchDescription) {
            if (notFoundOps.isEmpty() == false) {
                mismatchDescription.appendText("not found ").appendValueList("[", ", ", "]", notFoundOps);
            }
            if (notExpectedOps.isEmpty() == false) {
                if (notFoundOps.isEmpty() == false) {
                    mismatchDescription.appendText("; ");
                }
                mismatchDescription.appendText("not expected ").appendValueList("[", ", ", "]", notExpectedOps);
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("snapshot contains ").appendValueList("[", ", ", "]", expectedOps).appendText(" in any order.");
        }
    }

    static class ContainingSeqNoRangeMatcher extends TypeSafeMatcher<Translog.Snapshot> {
        private final long minSeqNo;
        private final long maxSeqNo;
        private final List<Long> notFoundSeqNo = new ArrayList<>();

        ContainingSeqNoRangeMatcher(long minSeqNo, long maxSeqNo) {
            this.minSeqNo = minSeqNo;
            this.maxSeqNo = maxSeqNo;
        }

        @Override
        protected boolean matchesSafely(Translog.Snapshot snapshot) {
            try {
                final Set<Long> seqNoList = new HashSet<>();
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    seqNoList.add(op.seqNo());
                }
                for (long i = minSeqNo; i <= maxSeqNo; i++) {
                    if (seqNoList.contains(i) == false) {
                        notFoundSeqNo.add(i);
                    }
                }
                return notFoundSeqNo.isEmpty();
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to read snapshot content", ex);
            }
        }

        @Override
        protected void describeMismatchSafely(Translog.Snapshot snapshot, Description mismatchDescription) {
            mismatchDescription.appendText("not found seqno ").appendValueList("[", ", ", "]", notFoundSeqNo);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("snapshot contains all seqno from [" + minSeqNo + " to " + maxSeqNo + "]");
        }
    }
}
