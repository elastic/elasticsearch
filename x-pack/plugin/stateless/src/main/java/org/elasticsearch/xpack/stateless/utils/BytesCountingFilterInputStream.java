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

package co.elastic.elasticsearch.stateless.utils;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class BytesCountingFilterInputStream extends FilterInputStream {

    private int bytesRead = 0;

    public BytesCountingFilterInputStream(InputStream in) {
        super(in);
    }

    @Override
    public int read() throws IOException {
        assert assertInvariant();
        final int result = super.read();
        if (result != -1) {
            bytesRead += 1;
        }
        return result;
    }

    // Not overriding read(byte[]) because FilterInputStream delegates to read(byte[], int, int)

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        assert assertInvariant();
        final int n = super.read(b, off, len);
        if (n != -1) {
            bytesRead += n;
        }
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        assert assertInvariant();
        final long skipped = super.skip(n);
        bytesRead += Math.toIntExact(skipped);
        return skipped;
    }

    public int getBytesRead() {
        return bytesRead;
    }

    protected boolean assertInvariant() {
        return true;
    }
}
