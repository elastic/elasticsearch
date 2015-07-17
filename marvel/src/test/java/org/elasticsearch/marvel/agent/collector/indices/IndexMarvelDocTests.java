/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

public class IndexMarvelDocTests extends ElasticsearchTestCase {

    @Test
    public void testCreateMarvelDoc() {
        String cluster = randomUnicodeOfLength(10);
        String type = randomUnicodeOfLength(10);
        long timestamp = randomLong();
        String index = randomUnicodeOfLength(10);
        long docsCount = randomLong();
        long storeSize = randomLong();
        long storeThrottle = randomLong();
        long indexingThrottle = randomLong();

        IndexMarvelDoc marvelDoc = IndexMarvelDoc.createMarvelDoc(cluster, type, timestamp,
                index, docsCount, storeSize, storeThrottle, indexingThrottle);

        assertNotNull(marvelDoc);
        assertThat(marvelDoc.clusterName(), equalTo(cluster));
        assertThat(marvelDoc.type(), equalTo(type));
        assertThat(marvelDoc.timestamp(), equalTo(timestamp));
        assertThat(marvelDoc.getIndex(), equalTo(index));
        assertNotNull(marvelDoc.getDocs());
        assertThat(marvelDoc.getDocs().getCount(), equalTo(docsCount));
        assertNotNull(marvelDoc.getStore());
        assertThat(marvelDoc.getStore().getSizeInBytes(), equalTo(storeSize));
        assertThat(marvelDoc.getStore().getThrottleTimeInMillis(), equalTo(storeThrottle));
        assertNotNull(marvelDoc.getIndexing());
        assertThat(marvelDoc.getIndexing().getThrottleTimeInMillis(), equalTo(indexingThrottle));
    }
}
