/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.dfs;

import com.carrotsearch.hppc.ObjectObjectHashMap;

import org.apache.lucene.search.CollectionStatistics;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;

public class DfsSearchResultTests extends ESTestCase {

    /**
     * checks inputs from 6.x that are difficult to simulate in a BWC mixed-cluster test, in particular the case
     * where docCount == -1L which does not occur with the codecs that we typically use.
     */
    public void test6xSerialization() throws IOException {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_8_0, Version.V_6_8_18);
        BytesStreamOutput os = new BytesStreamOutput();
        os.setVersion(version);
        os.writeVInt(1);
        String field = randomAlphaOfLength(10);
        os.writeString(field);
        long maxDoc = randomIntBetween(1, 5);
        os.writeVLong(maxDoc);
        long docCount = randomBoolean() ? -1 : randomIntBetween(1, (int) maxDoc);
        os.writeVLong(DfsSearchResult.addOne(docCount));
        long sumTotalTermFreq = randomBoolean() ? -1 : randomIntBetween(20, 30);
        os.writeVLong(DfsSearchResult.addOne(sumTotalTermFreq));
        long sumDocFreq = sumTotalTermFreq == -1 ? randomIntBetween(20, 30) : randomIntBetween(20, (int) sumTotalTermFreq);
        os.writeVLong(DfsSearchResult.addOne(sumDocFreq));

        try (StreamInput input = StreamInput.wrap(BytesReference.toBytes(os.bytes()))) {
            input.setVersion(version);
            ObjectObjectHashMap<String, CollectionStatistics> stats = DfsSearchResult.readFieldStats(input);
            assertEquals(stats.size(), 1);
            assertNotNull(stats.get(field));
            CollectionStatistics cs = stats.get(field);
            assertEquals(field, cs.field());
            assertEquals(maxDoc, cs.maxDoc());
            assertEquals(docCount == -1 ? maxDoc : docCount, cs.docCount());
            assertEquals(sumDocFreq, cs.sumDocFreq());
            assertEquals(sumTotalTermFreq == -1 ? sumDocFreq : sumTotalTermFreq, cs.sumTotalTermFreq());
        }
    }
}
