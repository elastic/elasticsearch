/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.recycler;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.Arrays;

public abstract class AbstractRecyclerTests extends ElasticsearchTestCase {

    protected static final Recycler.C<byte[]> RECYCLER_C = new Recycler.C<byte[]>() {

        @Override
        public byte[] newInstance(int sizing) {
            return new byte[10];
        }

        @Override
        public void clear(byte[] value) {
            Arrays.fill(value, (byte) 0);
        }

    };

    protected abstract Recycler<byte[]> newRecycler();

    public void testReuse() {
        Recycler<byte[]> r = newRecycler();
        Recycler.V<byte[]> o = r.obtain();
        final byte[] b1 = o.v();
        o.release();
        o = r.obtain();
        final byte[] b2 = o.v();
        if (o.isRecycled()) {
            assertSame(b1, b2);
        }
        o.release();
    }

    public void testClear() {
        Recycler<byte[]> r = newRecycler();
        Recycler.V<byte[]> o = r.obtain();
        getRandom().nextBytes(o.v());
        o.release();
        o = r.obtain();
        for (int i = 0; i < o.v().length; ++i) {
            assertEquals(0, o.v()[i]);
        }
        o.release();
    }

    public void testDoubleRelease() {
        final Recycler<byte[]> r = newRecycler();
        final Recycler.V<byte[]> v1 = r.obtain();
        v1.release();
        try {
            v1.release();
        } catch (ElasticSearchIllegalStateException e) {
            // impl has protection against double release: ok
            return;
        }
        // otherwise ensure that the impl may not be returned twice
        final Recycler.V<byte[]> v2 = r.obtain();
        final Recycler.V<byte[]> v3 = r.obtain();
        assertNotSame(v2.v(), v3.v());
    }

}
