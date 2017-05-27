

package org.elasticsearch.transport;

import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;

import java.io.IOException;

public class TCPTransportOutputStream extends BytesStreamOutput implements Releasable {

    private final ReleasableBytesStreamOutput bytesStreamOutput;
    private final boolean shouldCompress;
    private StreamOutput stream;

    public TCPTransportOutputStream(ReleasableBytesStreamOutput bytesStreamOutput, boolean shouldCompress) throws IOException {
        this.bytesStreamOutput = bytesStreamOutput;
        this.shouldCompress = shouldCompress;
        if (shouldCompress) {
            this.stream = CompressorFactory.COMPRESSOR.streamOutput(bytesStreamOutput);
        }
    }
}
