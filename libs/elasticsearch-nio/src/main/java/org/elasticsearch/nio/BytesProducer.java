package org.elasticsearch.nio;

import java.io.IOException;

public interface BytesProducer {

    void writeMessage(NewWriteOperation writeOperation) throws IOException;

    BytesWriteOperation pollBytes();

}
