

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

public class CompressibleBytesOutputStream extends StreamOutput implements Releasable {

    private final StreamOutput stream;
    private final ReleasableBytesStreamOutput bytesStreamOutput;
    private final boolean shouldCompress;

    public CompressibleBytesOutputStream(BigArrays bigArrays, boolean shouldCompress) throws IOException {
        bytesStreamOutput = new ReleasableBytesStreamOutput(bigArrays);
        this.shouldCompress = shouldCompress;
        if (shouldCompress) {
            this.stream = CompressorFactory.COMPRESSOR.streamOutput(Streams.flushOnCloseStream(bytesStreamOutput));
        } else {
            stream = bytesStreamOutput;
        }
    }

    /**
     * This method ensures that compression is complete and returns the underlying bytes.
     *
     * @return bytes underlying the stream
     * @throws IOException if an exception occurs when writing or flushing
     */
    public BytesStream finishStream() throws IOException {
        // If we are using compression the stream needs to be closed to ensure that EOS marker bytes are written.
        // The actual ReleasableBytesStreamOutput will not be closed yet it is wrapped in flushOnCloseStream when
        // passed to the deflater stream.
        if (shouldCompress) {
            stream.close();
        }

        return bytesStreamOutput;
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
