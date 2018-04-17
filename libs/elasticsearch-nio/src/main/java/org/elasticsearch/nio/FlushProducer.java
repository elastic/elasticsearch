package org.elasticsearch.nio;

import java.io.IOException;
import java.util.function.BiConsumer;

public interface FlushProducer {

    void produceWrites(WriteOperation writeOperation);

    FlushOperation pollFlushOperation();

    void close() throws IOException;

    WriteOperation createWriteOperation(SocketChannelContext channelContext, Object message, BiConsumer<Void, Throwable> listener);
}
