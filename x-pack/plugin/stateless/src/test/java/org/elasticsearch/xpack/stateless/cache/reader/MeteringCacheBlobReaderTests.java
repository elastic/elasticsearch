/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cache.reader;

import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.LongAdder;

public class MeteringCacheBlobReaderTests extends ESTestCase {

    public void testTotalBytesReadConsumer() throws IOException {

        var size = randomIntBetween(16, 1024);

        var cacheBlobReader = new CacheBlobReader() {
            @Override
            public ByteRange getRange(long position, int length, long remainingFileLength) {
                return null;
            }

            @Override
            public InputStream getRangeInputStream(long position, int length) throws IOException {
                byte[] buff = randomByteArrayOfLength(size);
                return new ByteArrayInputStream(buff);
            }
        };

        var accumulator = new LongAdder();
        var meteringCacheBlobReader = new MeteringCacheBlobReader(cacheBlobReader, accumulator::add);
        var meteredInputStream = meteringCacheBlobReader.getRangeInputStream(randomInt(), randomInt());

        if (randomBoolean()) {
            Streams.consumeFully(meteredInputStream);
            assertEquals(size, accumulator.longValue());
        } else {
            int limit = randomIntBetween(0, size);
            try (var is = Streams.limitStream(meteredInputStream, limit)) {
                // consume up to a limit (partially) and close the underlying input stream
                Streams.consumeFully(is);
            }
            assertEquals(limit, accumulator.longValue());
        }
    }
}
