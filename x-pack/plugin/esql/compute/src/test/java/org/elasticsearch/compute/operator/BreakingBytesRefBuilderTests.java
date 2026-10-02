/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.StreamOutputHelper;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class BreakingBytesRefBuilderTests extends ESTestCase {
    public void testBreakOnBuild() {
        String label = randomAlphaOfLength(4);
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(0));
        Exception e = expectThrows(CircuitBreakingException.class, () -> new BreakingBytesRefBuilder(breaker, label));
        assertThat(e.getMessage(), equalTo("over test limit"));
    }

    public void testAddByte() {
        testAgainstOracle(() -> new TestIteration() {
            final byte b = randomByte();

            @Override
            public void applyToBuilder(BreakingBytesRefBuilder builder) {
                builder.append(b);
            }

            @Override
            public void applyToOracle(BytesRefBuilder oracle) {
                oracle.append(b);
            }
        });
    }

    public void testAddBytesRef() {
        testAgainstOracle(() -> new TestIteration() {
            final BytesRef ref = new BytesRef(randomAlphaOfLengthBetween(1, 100));

            @Override
            public void applyToBuilder(BreakingBytesRefBuilder builder) {
                builder.append(ref);
            }

            @Override
            public void applyToOracle(BytesRefBuilder oracle) {
                oracle.append(ref);
            }
        });
    }

    public void testCopyBytes() {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(300));
        try (BreakingBytesRefBuilder builder = new BreakingBytesRefBuilder(breaker, "test")) {
            String initialValue = randomAlphaOfLengthBetween(1, 50);
            builder.copyBytes(new BytesRef(initialValue));
            assertThat(builder.bytesRefView().utf8ToString(), equalTo(initialValue));

            String newValue = randomAlphaOfLengthBetween(350, 500);
            Exception e = expectThrows(CircuitBreakingException.class, () -> builder.copyBytes(new BytesRef(newValue)));
            assertThat(e.getMessage(), equalTo("over test limit"));
        }
    }

    public void testCloseAllBatchesReleaseIntoOneCallAndReleasesTheArray() {
        var bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(200)).withCircuitBreaking();
        var breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        var countingBreaker = new ReleaseCountingCircuitBreaker(breaker);

        int numBuilders = between(1, 1000);
        ObjectArray<BreakingBytesRefBuilder> builders = bigArrays.newObjectArray(numBuilders);
        for (int i = 0; i < numBuilders; i++) {
            // Leave some slots null too -- closeAll must skip them, not just batch over them.
            // Index 0 is always populated so there's always at least one builder to release;
            // otherwise closeAll legitimately has nothing to release and skips the breaker call.
            if (i == 0 || randomBoolean()) {
                builders.set(i, new BreakingBytesRefBuilder(countingBreaker, "test", randomIntBetween(0, 64)));
            }
        }

        assertThat("breaker should be holding memory before closeAll", breaker.getUsed(), greaterThan(0L));

        long releaseCallsBefore = countingBreaker.releaseCalls.get();
        BreakingBytesRefBuilder.closeAll(builders);
        long releaseCallsDuring = countingBreaker.releaseCalls.get() - releaseCallsBefore;

        assertThat("breaker must be fully released after closeAll", breaker.getUsed(), equalTo(0L));
        assertThat("closeAll should batch every builder's release into a single breaker call", releaseCallsDuring, equalTo(1L));
    }

    public void testCloseAllAssertsAllBuildersShareOneBreaker() {
        var bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(200)).withCircuitBreaking();
        var breakerA = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        var breakerB = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(200));

        ObjectArray<BreakingBytesRefBuilder> builders = bigArrays.newObjectArray(2);
        var builderA = new BreakingBytesRefBuilder(breakerA, "test");
        var builderB = new BreakingBytesRefBuilder(breakerB, "test");
        builders.set(0, builderA);
        builders.set(1, builderB);

        try {
            // closeAll releases the array itself (in its own finally) even though the assertion
            // trips before it reaches its own breaker release -- the individual builders are
            // never released by closeAll in that case, so we close them ourselves below.
            expectThrows(AssertionError.class, () -> BreakingBytesRefBuilder.closeAll(builders));
        } finally {
            Releasables.closeExpectNoException(builderA, builderB);
        }
    }

    public void testGrow() {
        testAgainstOracle(() -> new TestIteration() {
            final int length = between(1, 100);
            final byte b = randomByte();

            @Override
            public void applyToBuilder(BreakingBytesRefBuilder builder) {
                builder.grow(builder.length() + length);
                builder.bytes()[builder.length()] = b;
                builder.setLength(builder.length() + length);
            }

            @Override
            public void applyToOracle(BytesRefBuilder oracle) {
                oracle.grow(oracle.length() + length);
                oracle.bytes()[oracle.length()] = b;
                oracle.setLength(oracle.length() + length);
            }
        });
    }

    public void testStream() {
        testAgainstOracle(() -> switch (between(0, 3)) {
            case 0 -> new XContentTestIteration() {
                @Override
                protected void apply(XContentBuilder builder) throws IOException {
                    // Noop
                }

                @Override
                public String toString() {
                    return "noop";
                }
            };
            case 1 -> new XContentTestIteration() {
                private final String value = randomAlphanumericOfLength(10);

                @Override
                protected void apply(XContentBuilder builder) throws IOException {
                    builder.value(value);
                }

                @Override
                public String toString() {
                    return '"' + value + '"';
                }
            };
            case 2 -> new XContentTestIteration() {
                private final long value = randomLong();

                @Override
                protected void apply(XContentBuilder builder) throws IOException {
                    builder.value(value);
                }

                @Override
                public String toString() {
                    return Long.toString(value);
                }
            };
            case 3 -> new XContentTestIteration() {
                private final String name = randomAlphanumericOfLength(5);
                private final String value = randomAlphanumericOfLength(5);

                @Override
                protected void apply(XContentBuilder builder) throws IOException {
                    builder.startObject().field(name, value).endObject();
                }

                @Override
                public String toString() {
                    return name + ": " + value;
                }
            };
            default -> throw new UnsupportedOperationException();
        });
    }

    private abstract static class XContentTestIteration implements TestIteration {
        protected abstract void apply(XContentBuilder builder) throws IOException;

        @Override
        public void applyToBuilder(BreakingBytesRefBuilder builder) {
            applyToStream(new StreamWrapper(builder));
        }

        @Override
        public void applyToOracle(BytesRefBuilder oracle) {
            try (var out = Streams.flushOnCloseStream(new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE))) {
                applyToStream(out);
                oracle.append(out.bytes().toBytesRef());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void applyToStream(StreamOutput out) {
            try {
                try (XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, out)) {
                    apply(builder);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static class StreamWrapper extends StreamOutput {

        private final BreakingBytesRefBuilder delegate;

        private StreamWrapper(BreakingBytesRefBuilder delegate) {
            this.delegate = delegate;
        }

        @Override
        public long position() {
            return delegate.length();
        }

        @Override
        public void writeByte(byte b) {
            delegate.append(b);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) {
            delegate.append(b, offset, length);
        }

        @Override
        public void writeString(String str) throws IOException {
            StreamOutputHelper.writeString(str, this);
        }

        @Override
        public void writeOptionalString(@Nullable String str) throws IOException {
            StreamOutputHelper.writeOptionalString(str, this);
        }

        @Override
        public void writeGenericString(String value) throws IOException {
            StreamOutputHelper.writeGenericString(value, this);
        }

        @Override
        public void flush() {}

        /**
         * Closes this stream to further operations. NOOP because we don't want to
         * close the builder when we close.
         */
        @Override
        public void close() {}
    }

    interface TestIteration {
        void applyToBuilder(BreakingBytesRefBuilder builder);

        void applyToOracle(BytesRefBuilder oracle);
    }

    private void testAgainstOracle(Supplier<TestIteration> iterations) {
        int limit = between(1_000, 10_000);
        String label = randomAlphaOfLength(4);
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(limit));
        assertThat(breaker.getUsed(), equalTo(0L));
        try (BreakingBytesRefBuilder builder = new BreakingBytesRefBuilder(breaker, label)) {
            assertThat(breaker.getUsed(), equalTo(builder.ramBytesUsed()));
            BytesRefBuilder oracle = new BytesRefBuilder();

            assertThat(builder.bytesRefView(), equalTo(oracle.get()));
            while (true) {
                TestIteration iteration = iterations.get();

                int prevOracle = oracle.length();
                iteration.applyToOracle(oracle);
                int size = oracle.length() - prevOracle;
                int targetSize = builder.length() + size;
                boolean willResize = targetSize >= builder.bytes().length;
                if (willResize) {
                    long resizeMemoryUsage = BreakingBytesRefBuilder.SHALLOW_SIZE + ramForArray(builder.bytes().length);
                    resizeMemoryUsage += ramForArray(ArrayUtil.oversize(targetSize, Byte.BYTES));
                    if (resizeMemoryUsage > limit) {
                        Exception e = expectThrows(CircuitBreakingException.class, () -> iteration.applyToBuilder(builder));
                        assertThat(e.getMessage(), equalTo("over test limit"));
                        break;
                    }
                }
                iteration.applyToBuilder(builder);
                assertThat(builder.bytesRefView(), equalTo(oracle.get()));
                assertThat(
                    builder.ramBytesUsed(),
                    // Label and breaker aren't counted in ramBytesUsed because they are usually shared with other instances.
                    equalTo(RamUsageTester.ramUsed(builder) - RamUsageTester.ramUsed(label) - RamUsageTester.ramUsed(breaker))
                );
                assertThat(builder.ramBytesUsed(), equalTo(breaker.getUsed()));
            }
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** Delegates to a real breaker, counting only release (negative-delta) calls to {@link #addWithoutBreaking(long)}. */
    private static class ReleaseCountingCircuitBreaker implements CircuitBreaker {
        private final CircuitBreaker delegate;
        final AtomicLong releaseCalls = new AtomicLong();

        ReleaseCountingCircuitBreaker(CircuitBreaker delegate) {
            this.delegate = delegate;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            delegate.circuitBreak(fieldName, bytesNeeded);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            delegate.addEstimateBytesAndMaybeBreak(bytes, label);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            if (bytes < 0) {
                releaseCalls.incrementAndGet();
            }
            delegate.addWithoutBreaking(bytes);
        }

        @Override
        public long getUsed() {
            return delegate.getUsed();
        }

        @Override
        public long getLimit() {
            return delegate.getLimit();
        }

        @Override
        public double getOverhead() {
            return delegate.getOverhead();
        }

        @Override
        public long getTrippedCount() {
            return delegate.getTrippedCount();
        }

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public Durability getDurability() {
            return delegate.getDurability();
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            delegate.setLimitAndOverhead(limit, overhead);
        }
    }

    private long ramForArray(int length) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + length);
    }
}
