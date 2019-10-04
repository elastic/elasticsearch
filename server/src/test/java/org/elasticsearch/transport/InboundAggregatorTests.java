package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

public class InboundAggregatorTests extends ESTestCase {

    private final TestThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final AtomicReference<AggregatedMessage> message = new AtomicReference<>();
    private InboundAggregator aggregator;

    @Before
    @Override
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        aggregator = new InboundAggregator(message::set);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testCannotReceiveHeaderTwice() {
        long requestId = randomLong();
        Header header = new Header(randomInt(), requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
        aggregator.headerReceived(header);

        expectThrows(IllegalStateException.class, () -> aggregator.headerReceived(header));
    }

    public void testCannotReceiveContentWithoutHeader() throws IOException {
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            threadPool.getThreadContext().writeTo(streamOutput);
            streamOutput.writeString("action_name");
            streamOutput.write(randomByteArrayOfLength(10));
            expectThrows(IllegalStateException.class, () -> {
                ReleasableBytesReference content = new ReleasableBytesReference(streamOutput.bytes(), () -> {});
                aggregator.contentReceived(content);
            });
        }
    }

    public void testInboundAggregation() throws IOException {
        long requestId = randomLong();
        Header header = new Header(randomInt(), requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
        // Initiate Message
        aggregator.headerReceived(header);

        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            threadPool.getThreadContext().writeTo(streamOutput);
            streamOutput.writeString("action_name");
            streamOutput.write(randomByteArrayOfLength(10));
            aggregator.contentReceived(new ReleasableBytesReference(streamOutput.bytes(), () -> {}));
        }

        aggregator.contentReceived(new ReleasableBytesReference(new BytesArray(randomByteArrayOfLength(10)), () -> {}));

        assertThat(message.get(), nullValue());

        // Signal EOS
        aggregator.contentReceived(new ReleasableBytesReference(BytesArray.EMPTY, () -> {}));

        assertThat(message.get(), notNullValue());
        assertFalse(message.get().isPing());
        assertTrue(message.get().getHeader().isRequest());
        assertThat(message.get().getHeader().getRequestId(), equalTo(requestId));
        assertThat(message.get().getHeader().getVersion(), equalTo(Version.CURRENT));
    }
}
