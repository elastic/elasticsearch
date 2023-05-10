/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.tdigest;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Verifies that the various TDigest implementations can be serialized.
 * <p>
 * Serializability is important, for example, if we want to use t-digests with Spark.
 */
public class TDigestSerializationTest {
    @Test
    public void testMergingDigest() throws IOException {
        assertSerializesAndDeserializes(new MergingDigest(100));
    }

    @Test
    public void testAVLTreeDigest() throws IOException {
        assertSerializesAndDeserializes(new AVLTreeDigest(100));
    }

    private <T extends TDigest> void assertSerializesAndDeserializes(T tdigest) throws IOException {
        assertNotNull(deserialize(serialize(tdigest)));

        final Random gen = new Random();
        for (int i = 0; i < 100000; i++) {
            tdigest.add(gen.nextDouble());
        }
        T roundTrip = deserialize(serialize(tdigest));

        assertTDigestEquals(tdigest, roundTrip);
    }

    private static byte[] serialize(Serializable obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(5120);
        try (ObjectOutputStream out = new ObjectOutputStream(baos)){
            out.writeObject(obj);
            return baos.toByteArray();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T deserialize(byte[] objectData) throws IOException {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(objectData))) {
            return (T) in.readObject();
        } catch (ClassCastException | ClassNotFoundException | IOException e) {
            throw new IOException(e);
        }
    }

    private void assertTDigestEquals(TDigest t1, TDigest t2) {
        assertEquals(t1.getMin(), t2.getMin(), 0);
        assertEquals(t1.getMax(), t2.getMax(), 0);
        Iterator<Centroid> cx = t2.centroids().iterator();
        for (Centroid c1 : t1.centroids()) {
            Centroid c2 = cx.next();
            assertEquals(c1.count(), c2.count());
            assertEquals(c1.mean(), c2.mean(), 1e-10);
        }
        assertFalse(cx.hasNext());
        assertNotNull(t2);
    }
}
