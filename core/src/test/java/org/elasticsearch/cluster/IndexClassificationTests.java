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

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ElasticsearchTestCase;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class IndexClassificationTests extends ElasticsearchTestCase {

    @Test
    public void testIndexSizeClassification() throws Exception {
        testSequence(1, 2, 3, 4, 5);
        testSequence(1, 5, 10, 15, 20);
        testSequence(1, 17, 27, 45, 55);

        Map<String, Long> indexSizes = new HashMap<>(2);

        indexSizes.put("single", 1L);
        Map<String, ClusterInfo.IndexSize> c = IndexClassification.classifyIndices(indexSizes, logger).getIndexClassifications();
        assertTrue(c.get("single") == ClusterInfo.IndexSize.MEDIUM);

        indexSizes.clear();

        indexSizes.put("foo", 3L);
        indexSizes.put("bar", 3L);
        c = IndexClassification.classifyIndices(indexSizes, logger).getIndexClassifications();
        assertTrue(c.get("foo") == ClusterInfo.IndexSize.MEDIUM);
        assertTrue(c.get("bar") == ClusterInfo.IndexSize.MEDIUM);
    }

    @Test
    public void testIndexSizeStreaming() throws Exception {
        Map<String, Long> indexSizes = new HashMap<>(2);

        indexSizes.put("foo", 1L);
        indexSizes.put("bar", 100L);
        IndexClassification ic = IndexClassification.classifyIndices(indexSizes, logger);
        assertTrue(ic.getIndexClassifications().get("foo") == ClusterInfo.IndexSize.SMALLEST);
        assertTrue(ic.getIndexClassifications().get("bar") == ClusterInfo.IndexSize.LARGEST);

        BytesStreamOutput out = new BytesStreamOutput();
        ic.writeTo(out);
        out.flush();
        ByteBufferStreamInput in = new ByteBufferStreamInput(ByteBuffer.wrap(out.bytes().toBytes()));
        IndexClassification ic2 = new IndexClassification();
        ic2.readFrom(in);

        assertTrue(ic.getIndexClassifications().get("foo") == ClusterInfo.IndexSize.SMALLEST);
        assertTrue(ic.getIndexClassifications().get("bar") == ClusterInfo.IndexSize.LARGEST);

        assertEquals(ic.hashCode(), ic2.hashCode());
        assertEquals(ic, ic2);
    }

    private void testSequence(long tiny, long small, long medium, long large, long huge) {
        Map<String, Long> indexSizes = new HashMap<>(5);

        indexSizes.put("tiny", tiny);
        indexSizes.put("small", small);
        indexSizes.put("medium", medium);
        indexSizes.put("large", large);
        indexSizes.put("huge", huge);
        Map<String, ClusterInfo.IndexSize> c = IndexClassification.classifyIndices(indexSizes, logger).getIndexClassifications();

        assertTrue("should be SMALLEST:" + c.get("tiny"),
                c.get("tiny") == ClusterInfo.IndexSize.SMALLEST);
        assertTrue("should be SMALL:" + c.get("small"),
                c.get("small") == ClusterInfo.IndexSize.SMALL);
        assertTrue("should be MEDIUM:" + c.get("medium"),
                c.get("medium") == ClusterInfo.IndexSize.MEDIUM);
        assertTrue("should be LARGE:" + c.get("large"),
                c.get("large") == ClusterInfo.IndexSize.LARGE);
        assertTrue("should be LARGEST:" + c.get("huge"),
                c.get("huge") == ClusterInfo.IndexSize.LARGEST);
    }
}
