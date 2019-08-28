package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.CoreMatchers;
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
    private final AtomicReference<Tuple<Header, StreamInput>> message = new AtomicReference<>();
    private InboundAggregator aggregator;

    @Before
    @Override
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        aggregator = new InboundAggregator((h, m) -> message.set(new Tuple<>(h, m)));
    }

    @After
    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testInboundAggregation() throws IOException {
        long requestId = randomLong();
        Header header = new Header(requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
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
        assertTrue(message.get().v1().isFullyParsed());
        assertTrue(message.get().v1().isRequest());
        assertThat(message.get().v1().getRequestId(), equalTo(requestId));
        assertThat(message.get().v1().getVersion(), equalTo(Version.CURRENT));

        
    }
}
