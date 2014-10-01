/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.threadpool;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class ThreadPoolSerializationTests extends ElasticsearchTestCase {

    BytesStreamOutput output = new BytesStreamOutput();

    @Test
    public void testThatQueueSizeSerializationWorks() throws Exception {
        ThreadPool.Info info = new ThreadPool.Info("foo", "search", 1, 10, TimeValue.timeValueMillis(3000), SizeValue.parseSizeValue("10k"));
        output.setVersion(Version.CURRENT);
        info.writeTo(output);

        StreamInput input = new BytesStreamInput(output.bytes());
        ThreadPool.Info newInfo = new ThreadPool.Info();
        newInfo.readFrom(input);

        assertThat(newInfo.getQueueSize().singles(), is(10000l));
    }

    @Test
    public void testThatNegativeQueueSizesCanBeSerialized() throws Exception {
        ThreadPool.Info info = new ThreadPool.Info("foo", "search", 1, 10, TimeValue.timeValueMillis(3000), null);
        output.setVersion(Version.CURRENT);
        info.writeTo(output);

        StreamInput input = new BytesStreamInput(output.bytes());
        ThreadPool.Info newInfo = new ThreadPool.Info();
        newInfo.readFrom(input);

        assertThat(newInfo.getQueueSize(), is(nullValue()));
    }

    @Test
    public void testThatToXContentWritesOutUnboundedCorrectly() throws Exception {
        ThreadPool.Info info = new ThreadPool.Info("foo", "search", 1, 10, TimeValue.timeValueMillis(3000), null);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        info.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        BytesReference bytesReference = builder.bytes();
        XContentParser parser = XContentFactory.xContent(bytesReference).createParser(bytesReference);
        Map<String, Object> map = parser.mapAndClose();
        assertThat(map, hasKey("foo"));
        map = (Map<String, Object>) map.get("foo");
        assertThat(map, hasKey("queue_size"));
        assertThat(map.get("queue_size").toString(), is("-1"));
    }

    @Test
    public void testThatNegativeSettingAllowsToStart() throws InterruptedException {
        Settings settings = settingsBuilder().put("name", "index").put("threadpool.index.queue_size", "-1").build();
        ThreadPool threadPool = new ThreadPool(settings, null);
        assertThat(threadPool.info("index").getQueueSize(), is(nullValue()));
        terminate(threadPool);
    }
}
