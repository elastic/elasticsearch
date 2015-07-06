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
package org.elasticsearch;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.ThrowableObjectInputStream;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.junit.Test;

import java.io.*;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.io.ThrowableObjectOutputStream.serialize;

public class ExceptionsSerializationTests extends ElasticsearchTestCase {

    public void testBasicExceptions() throws IOException, ClassNotFoundException {
        ShardId id = new ShardId("foo", 1);
        DiscoveryNode src = new DiscoveryNode("someNode", new InetSocketTransportAddress("127.0.0.1", 6666), Version.CURRENT);
        DiscoveryNode target = new DiscoveryNode("otherNode", new InetSocketTransportAddress("127.0.0.1", 8888), Version.CURRENT);

        RecoveryFailedException ex = new RecoveryFailedException(id, src, target, new AlreadyClosedException("closed", new SecurityException("booom booom boom", new FileNotFoundException("no such file"))));
        RecoveryFailedException serialize = serialize(ex);
        assertEquals(ex.getMessage(), serialize.getMessage());
        assertEquals(AlreadyClosedException.class, serialize.getCause().getClass());
        assertEquals(SecurityException.class, serialize.getCause().getCause().getClass());
        assertEquals(FileNotFoundException.class, serialize.getCause().getCause().getCause().getClass());
        ConnectTransportException tpEx = new ConnectTransportException(src, "foo", new IllegalArgumentException("boom"));
        ConnectTransportException serializeTpEx = serialize(tpEx);
        assertEquals(tpEx.getMessage(), serializeTpEx.getMessage());
        assertEquals(src, tpEx.node());

        TestException testException = new TestException(Arrays.asList("foo"), EnumSet.allOf(IndexShardState.class), ImmutableMap.<String,String>builder().put("foo", "bar").build(), InetAddress.getByName("localhost"), new Number[] {new Integer(1)});
        assertEquals(serialize(testException).list.get(0), "foo");
        assertTrue(serialize(testException).set.containsAll(Arrays.asList(IndexShardState.values())));
        assertEquals(serialize(testException).map.get("foo"), "bar");
    }

    public void testPreventBogusFromSerializing() throws IOException, ClassNotFoundException {
        Serializable[] serializables = new Serializable[] {
                new AtomicBoolean(false),
                TypeToken.of(String.class),
        };
        for (Serializable s : serializables) {
            try {
                serialize(s);
                fail(s.getClass() + " should fail");
            } catch (NotSerializableException e) {
                // all is well
            }
        }
    }

    public static class TestException extends Throwable {
        final List<String> list;
        final EnumSet<IndexShardState> set;
        final Map<String, String> map;
        final InetAddress address;
        final Object[] someArray;

        public TestException(List<String> list, EnumSet<IndexShardState> set, Map<String, String> map, InetAddress address, Object[] someArray) {
            super("foo", null);
            this.list = list;
            this.set = set;
            this.map = map;
            this.address = address;
            this.someArray = someArray;
        }
    }
}
