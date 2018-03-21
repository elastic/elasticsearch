package org.elasticsearch.nio;

import java.io.IOException;

public interface BytesProducer {

    void writeMessage(WriteOperation writeOperation) throws IOException;

    FlushOperation pollBytes();

}
