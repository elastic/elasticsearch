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

package org.elasticsearch.network;

import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/**
 */
public class DirectBufferNetworkTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
            .put(Node.HTTP_ENABLED, true)
            .put(super.nodeSettings(nodeOrdinal)).build();
    }

    /**
     * This test validates that using large data sets (large docs + large API requests) don't
     * cause a large direct byte buffer to be allocated internally in the sun.nio buffer cache.
     * <p/>
     * See {@link org.elasticsearch.common.netty.NettyUtils#DEFAULT_GATHERING} for more info.
     */
    @Test
    public void verifySaneDirectBufferAllocations() throws Exception {
        createIndex("test");

        int estimatedBytesSize = scaledRandomIntBetween(ByteSizeValue.parseBytesSizeValue("1.1mb").bytesAsInt(), ByteSizeValue.parseBytesSizeValue("1.5mb").bytesAsInt());
        byte[] data = new byte[estimatedBytesSize];
        getRandom().nextBytes(data);

        ByteArrayOutputStream docOut = new ByteArrayOutputStream();
        // we use smile to automatically use the binary mapping
        XContentBuilder doc = XContentFactory.smileBuilder(docOut).startObject().startObject("doc").field("value", data).endObject();
        doc.close();
        byte[] docBytes = docOut.toByteArray();

        int numDocs = randomIntBetween(2, 5);
        logger.info("indexing [{}] docs, each with size [{}]", numDocs, estimatedBytesSize);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; ++i) {
            builders[i] = client().prepareIndex("test", "type").setSource(docBytes);
        }
        indexRandom(true, builders);
        logger.info("done indexing");

        logger.info("executing random client search for all docs");
        assertHitCount(client().prepareSearch("test").setFrom(0).setSize(numDocs).get(), numDocs);
        logger.info("executing transport client search for all docs");
        assertHitCount(internalCluster().transportClient().prepareSearch("test").setFrom(0).setSize(numDocs).get(), numDocs);

        logger.info("executing HTTP search for all docs");
        // simulate large HTTP call as well
        httpClient().method("GET").path("/test/_search").addParam("size", Integer.toString(numDocs)).execute();

        logger.info("validating large direct buffer not allocated");
        validateNoLargeDirectBufferAllocated();
    }
    
    /**
     * Validates that all the thread local allocated ByteBuffer in sun.nio under the Util$BufferCache
     * are not greater than 1mb.
     */
    private void validateNoLargeDirectBufferAllocated() throws Exception {
        // Make the fields in the Thread class that store ThreadLocals
        // accessible
        Field threadLocalsField = Thread.class.getDeclaredField("threadLocals");
        threadLocalsField.setAccessible(true);
        // Make the underlying array of ThreadLoad.ThreadLocalMap.Entry objects
        // accessible
        Class<?> tlmClass = Class.forName("java.lang.ThreadLocal$ThreadLocalMap");
        Field tableField = tlmClass.getDeclaredField("table");
        tableField.setAccessible(true);

        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread == null) {
                continue;
            }
            Object threadLocalMap = threadLocalsField.get(thread);
            if (threadLocalMap == null) {
                continue;
            }
            Object[] table = (Object[]) tableField.get(threadLocalMap);
            if (table == null) {
                continue;
            }
            for (Object entry : table) {
                if (entry == null) {
                    continue;
                }
                Field valueField = entry.getClass().getDeclaredField("value");
                valueField.setAccessible(true);
                Object value = valueField.get(entry);
                if (value == null) {
                    continue;
                }
                if (!value.getClass().getName().equals("sun.nio.ch.Util$BufferCache")) {
                    continue;
                }
                Field buffersField = value.getClass().getDeclaredField("buffers");
                buffersField.setAccessible(true);
                Object[] buffers = (Object[]) buffersField.get(value);
                for (Object buffer : buffers) {
                    if (buffer == null) {
                        continue;
                    }
                    assertThat(((ByteBuffer) buffer).capacity(), Matchers.lessThan(1 * 1024 * 1024));
                }
            }
        }

    }
}
