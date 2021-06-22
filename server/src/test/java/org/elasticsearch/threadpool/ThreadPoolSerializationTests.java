/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.threadpool;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ThreadPoolSerializationTests extends ESTestCase {
    private final BytesStreamOutput output = new BytesStreamOutput();
    private ThreadPool.ThreadPoolType threadPoolType;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPoolType = randomFrom(ThreadPool.ThreadPoolType.values());
    }

    public void testThatQueueSizeSerializationWorks() throws Exception {
        ThreadPool.Info info = new ThreadPool.Info("foo", threadPoolType, 1, 10,
                TimeValue.timeValueMillis(3000), SizeValue.parseSizeValue("10k"));
        output.setVersion(Version.CURRENT);
        info.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ThreadPool.Info newInfo = new ThreadPool.Info(input);

        assertThat(newInfo.getQueueSize().singles(), is(10000L));
    }

    public void testThatNegativeQueueSizesCanBeSerialized() throws Exception {
        ThreadPool.Info info = new ThreadPool.Info("foo", threadPoolType, 1, 10, TimeValue.timeValueMillis(3000), null);
        output.setVersion(Version.CURRENT);
        info.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ThreadPool.Info newInfo = new ThreadPool.Info(input);

        assertThat(newInfo.getQueueSize(), is(nullValue()));
    }

    public void testThatToXContentWritesOutUnboundedCorrectly() throws Exception {
        ThreadPool.Info info = new ThreadPool.Info("foo", threadPoolType, 1, 10, TimeValue.timeValueMillis(3000), null);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        info.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        assertThat(map, hasKey("foo"));
        map = (Map<String, Object>) map.get("foo");
        assertThat(map, hasKey("queue_size"));
        assertThat(map.get("queue_size").toString(), is("-1"));
    }

    public void testThatNegativeSettingAllowsToStart() throws InterruptedException {
        Settings settings = Settings.builder().put("node.name", "write").put("thread_pool.write.queue_size", "-1").build();
        ThreadPool threadPool = new ThreadPool(settings);
        assertThat(threadPool.info("write").getQueueSize(), is(nullValue()));
        terminate(threadPool);
    }

    public void testThatToXContentWritesInteger() throws Exception {
        ThreadPool.Info info = new ThreadPool.Info("foo", threadPoolType, 1, 10,
                TimeValue.timeValueMillis(3000), SizeValue.parseSizeValue("1k"));
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        info.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        assertThat(map, hasKey("foo"));
        map = (Map<String, Object>) map.get("foo");
        assertThat(map, hasKey("queue_size"));
        assertThat(map.get("queue_size").toString(), is("1000"));
    }

    public void testThatThreadPoolTypeIsSerializedCorrectly() throws IOException {
        ThreadPool.Info info = new ThreadPool.Info("foo", threadPoolType);
        output.setVersion(Version.CURRENT);
        info.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ThreadPool.Info newInfo = new ThreadPool.Info(input);

        assertThat(newInfo.getThreadPoolType(), is(threadPoolType));
    }
}
