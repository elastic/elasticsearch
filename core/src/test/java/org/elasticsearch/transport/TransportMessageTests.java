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

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class TransportMessageTests extends ESTestCase {

    @Test
    public void testSerialization() throws Exception {
        Message message = new Message();
        message.putHeader("key1", "value1");
        message.putHeader("key2", "value2");
        message.putInContext("key3", "value3");

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.CURRENT);
        message.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes());
        in.setVersion(Version.CURRENT);
        message = new Message();
        message.readFrom(in);
        assertThat(message.getHeaders().size(), is(2));
        assertThat((String) message.getHeader("key1"), equalTo("value1"));
        assertThat((String) message.getHeader("key2"), equalTo("value2"));
        assertThat(message.isContextEmpty(), is(true));

        // ensure that casting is not needed
        String key1 = message.getHeader("key1");
        assertThat(key1, is("value1"));
    }

    @Test
    public void testCopyHeadersAndContext() throws Exception {
        Message m1 = new Message();
        m1.putHeader("key1", "value1");
        m1.putHeader("key2", "value2");
        m1.putInContext("key3", "value3");

        Message m2 = new Message(m1);

        assertThat(m2.getHeaders().size(), is(2));
        assertThat((String) m2.getHeader("key1"), equalTo("value1"));
        assertThat((String) m2.getHeader("key2"), equalTo("value2"));
        assertThat((String) m2.getFromContext("key3"), equalTo("value3"));

        // ensure that casting is not needed
        String key3 = m2.getFromContext("key3");
        assertThat(key3, is("value3"));
        testContext(m2, "key3", "value3");
    }

    // ensure that generic arg like this is not needed: TransportMessage<?> transportMessage
    private void testContext(TransportMessage transportMessage, String key, String expectedValue) {
        String result = transportMessage.getFromContext(key);
        assertThat(result, is(expectedValue));

    }

    private static class Message extends TransportMessage<Message> {

        private Message() {
        }

        private Message(Message message) {
            super(message);
        }
    }
}
