

package org.elasticsearch.transport;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.BigArrays;

import java.io.IOException;

public class CompressibleBytesOutputStream extends BytesStream implements Releasable {

    private final StreamOutput stream;
    private final ReleasableBytesStreamOutput bytesStreamOutput;
    private final boolean shouldCompress;
    private boolean finishCalled = false;

    public CompressibleBytesOutputStream(BigArrays bigArrays, boolean shouldCompress) throws IOException {
        bytesStreamOutput = new ReleasableBytesStreamOutput(bigArrays);
        this.shouldCompress = shouldCompress;
        if (shouldCompress) {
            this.stream = CompressorFactory.COMPRESSOR.streamOutput(Streams.flushOnCloseStream(bytesStreamOutput));
        } else {
            stream = bytesStreamOutput;
        }
    }

    public void finishStream() throws IOException {
        this.finishCalled = true;

        if (shouldCompress) {
            stream.close();
        }
    }

    @Override
    public BytesReference bytes() {
        assert finishCalled : "Must call finishStream() before accessing underlying bytes";
        return bytesStreamOutput.bytes();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        stream.write(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        stream.writeBytes(b, offset, length);
    }

    @Override
    public void flush() throws IOException {
        stream.flush();
    }

    @Override
    public void close() {
        // If we are not using compression stream == bytesStreamOutput
        if (shouldCompress) {
            IOUtils.closeWhileHandlingException(stream);
        }
        bytesStreamOutput.close();
    }

    @Override
    public void reset() throws IOException {
        stream.reset();
    }
}
