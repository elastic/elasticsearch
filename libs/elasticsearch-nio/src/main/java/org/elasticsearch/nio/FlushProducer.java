package org.elasticsearch.nio;

import java.io.IOException;

public interface FlushProducer {

    void produceWrites(WriteOperation writeOperation);

    FlushOperation pollFlushOperation();

    void close() throws IOException;

}
