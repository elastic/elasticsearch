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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SerializationTest {
    @Test
    public void mergingDigestSerDes() {
        final TDigest out = MergingDigest.createDigest(100);
        out.add(42.5);
        out.add(1);
        out.add(24.0);

        final ByteBuffer output = ByteBuffer.allocate(out.smallByteSize());
        out.asSmallBytes(output);

        ByteBuffer input = ByteBuffer.wrap(output.array());
        try {
            TDigest m = MergingDigest.fromBytes(input);
            for (double q = 0; q <= 1; q+=0.001) {
                assertEquals(m.quantile(q), out.quantile(q), 0);
            }
            Iterator<Centroid> ix = m.centroids().iterator();
            for (Centroid centroid : out.centroids()) {
                assertTrue(ix.hasNext());
                Centroid c = ix.next();
                assertEquals(centroid.mean(), c.mean(), 0);
                assertEquals(centroid.count(), c.count(), 0);
            }
            assertFalse(ix.hasNext());
        } catch (BufferUnderflowException e) {
            System.out.println("WTF?");
        }

        input = ByteBuffer.wrap(output.array());
        final TDigest in = MergingDigest.fromBytes(input);
        assertEquals(out.quantile(0.95), in.quantile(0.95), 0.001);
    }
}
