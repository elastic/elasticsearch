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

package org.elasticsearch.percolator;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.junit.Assert;

import static org.hamcrest.Matchers.greaterThan;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertVersionSerializable;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.formatShardStatus;

/** Static method pulled out of PercolatorIT, used by other tests */
public class PercolatorTestUtil extends Assert {

    public static PercolateRequestBuilder preparePercolate(ElasticsearchClient client) {
        return new PercolateRequestBuilder(client, PercolateAction.INSTANCE);
    }

    public static MultiPercolateRequestBuilder prepareMultiPercolate(ElasticsearchClient client) {
        return new MultiPercolateRequestBuilder(client, MultiPercolateAction.INSTANCE);
    }

    public static void assertMatchCount(PercolateResponse percolateResponse, long expectedHitCount) {
        if (percolateResponse.getCount() != expectedHitCount) {
            fail("Count is " + percolateResponse.getCount() + " but " + expectedHitCount + " was expected. " +
                    formatShardStatus(percolateResponse));
        }
        assertVersionSerializable(percolateResponse);
    }

    public static String[] convertFromTextArray(PercolateResponse.Match[] matches, String index) {
        if (matches.length == 0) {
            return Strings.EMPTY_ARRAY;
        }
        String[] strings = new String[matches.length];
        for (int i = 0; i < matches.length; i++) {
            assertEquals(index, matches[i].getIndex().string());
            strings[i] = matches[i].getId().string();
        }
        return strings;
    }

}
