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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.hamcrest.CoreMatchers.equalTo;

public class ReplicationResponseTests extends ESTestCase {

    public void testShardInfoToString() {
        final int total = 5;
        final int successful = randomIntBetween(1, total);
        final ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(total, successful);
        assertThat(
            shardInfo.toString(),
            equalTo(String.format(Locale.ROOT, "ShardInfo{total=5, successful=%d, failures=[]}", successful)));
    }

}
