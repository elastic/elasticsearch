/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.apache.lucene.util.BytesRef;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogramTestUtils.randomHistogram;
import static org.hamcrest.Matchers.equalTo;

public class CompressedExponentialHistogramTests extends ExponentialHistogramTestCase {

    public void testEncodeDecodeRandomHistogram() throws IOException {
        ReleasableExponentialHistogram input = randomHistogramWithDoubleZeroThreshold();

        CompressedExponentialHistogram decoded = toCompressedHistogram(input);

        assertThat(decoded, equalTo(input));
    }

    private static CompressedExponentialHistogram toCompressedHistogram(ReleasableExponentialHistogram input) throws IOException {
        ByteArrayOutputStream encodedStream = new ByteArrayOutputStream();
        if (randomBoolean()) {
            CompressedExponentialHistogram.writeHistogramBytes(
                encodedStream,
                input.scale(),
                input.negativeBuckets().iterator(),
                input.positiveBuckets().iterator()
            );
        } else {
            CompressedExponentialHistogram.writeHistogramBytes(encodedStream, input);
        }
        CompressedExponentialHistogram decoded = new CompressedExponentialHistogram();
        byte[] encodedBytes = encodedStream.toByteArray();
        decoded.reset(
            input.zeroBucket().zeroThreshold(),
            input.valueCount(),
            input.sum(),
            input.min(),
            input.max(),
            newBytesRef(encodedBytes)
        );
        return decoded;
    }

    private ReleasableExponentialHistogram randomHistogramWithDoubleZeroThreshold() {
        ExponentialHistogram random = randomHistogram();
        // Compressed histogram are lossy for index-based zero thresholds, so ensure we use a double-based one
        ReleasableExponentialHistogram input = ExponentialHistogram.builder(random, breaker())
            .zeroBucket(ZeroBucket.create(random.zeroBucket().zeroThreshold(), random.zeroBucket().count()))
            .build();
        autoReleaseOnTestEnd(input);
        return input;
    }

    public void testWriteHistogramBytesFastPath() throws IOException {
        ReleasableExponentialHistogram original = randomHistogramWithDoubleZeroThreshold();
        CompressedExponentialHistogram compressed = toCompressedHistogram(original);

        // Wrap in a subclass that throws on bucket access to prove the fast path copies raw bytes
        BucketAccessForbiddenHistogram forbidden = new BucketAccessForbiddenHistogram();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompressedExponentialHistogram.writeHistogramBytes(out, compressed);
        BytesRef encodedBytes = new BytesRef(out.toByteArray());
        forbidden.reset(
            compressed.zeroBucket().zeroThreshold(),
            compressed.valueCount(),
            compressed.sum(),
            compressed.min(),
            compressed.max(),
            encodedBytes
        );

        ByteArrayOutputStream reEncodedStream = new ByteArrayOutputStream();
        CompressedExponentialHistogram.writeHistogramBytes(reEncodedStream, forbidden);
        CompressedExponentialHistogram roundTripped = new CompressedExponentialHistogram();
        roundTripped.reset(
            forbidden.zeroBucket().zeroThreshold(),
            forbidden.valueCount(),
            forbidden.sum(),
            forbidden.min(),
            forbidden.max(),
            new BytesRef(reEncodedStream.toByteArray())
        );
        assertThat(roundTripped, equalTo(original));
    }

    /**
     * A {@link CompressedExponentialHistogram} subclass that throws on any bucket iterator access,
     * ensuring that the {@link CompressedExponentialHistogram#writeHistogramBytes(java.io.OutputStream, ExponentialHistogram)}
     * fast path copies raw bytes without iterating buckets.
     */
    private static class BucketAccessForbiddenHistogram extends CompressedExponentialHistogram {
        @Override
        public ExponentialHistogram.Buckets positiveBuckets() {
            throw new AssertionError("fast path should not access positiveBuckets()");
        }

        @Override
        public ExponentialHistogram.Buckets negativeBuckets() {
            throw new AssertionError("fast path should not access negativeBuckets()");
        }
    }

    public void testIteratorCopy() throws IOException {
        ReleasableExponentialHistogram input = randomHistogramWithDoubleZeroThreshold();
        CompressedExponentialHistogram decoded = toCompressedHistogram(input);

        assertThat(decoded, equalTo(input));

        CopyableBucketIterator it = decoded.positiveBuckets().iterator();

        int skipBuckets = randomIntBetween(0, decoded.positiveBuckets().bucketCount());
        for (int i = 0; i < skipBuckets; i++) {
            it.advance();
        }

        BucketIterator copy = it.copy();
        while (it.hasNext()) {
            assertThat(copy.hasNext(), equalTo(true));
            assertThat(copy.peekIndex(), equalTo(it.peekIndex()));
            assertThat(copy.peekCount(), equalTo(it.peekCount()));
            it.advance();
            copy.advance();
        }
        assertThat(copy.hasNext(), equalTo(false));
    }
}
