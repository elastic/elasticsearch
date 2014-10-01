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
package org.elasticsearch.indices.recovery;

import org.elasticsearch.test.ElasticsearchTestCase;

import static org.hamcrest.Matchers.closeTo;

public class RecoveryStateTest extends ElasticsearchTestCase {

    public void testPercentage() {
        RecoveryState state = new RecoveryState();
        RecoveryState.Index index = state.getIndex();
        index.totalByteCount(100);
        index.reusedByteCount(20);
        index.recoveredByteCount(80);
        assertThat((double)index.percentBytesRecovered(), closeTo(80.0d, 0.1d));

        index.totalFileCount(100);
        index.reusedFileCount(80);
        index.recoveredFileCount(20);
        assertThat((double)index.percentFilesRecovered(), closeTo(20.0d, 0.1d));

        index.totalByteCount(0);
        index.reusedByteCount(0);
        index.recoveredByteCount(0);
        assertThat((double)index.percentBytesRecovered(), closeTo(0d, 0.1d));

        index.totalFileCount(0);
        index.reusedFileCount(0);
        index.recoveredFileCount(0);
        assertThat((double)index.percentFilesRecovered(), closeTo(00.0d, 0.1d));

        index.totalByteCount(10);
        index.reusedByteCount(0);
        index.recoveredByteCount(10);
        assertThat((double)index.percentBytesRecovered(), closeTo(100d, 0.1d));

        index.totalFileCount(20);
        index.reusedFileCount(0);
        index.recoveredFileCount(20);
        assertThat((double)index.percentFilesRecovered(), closeTo(100.0d, 0.1d));
    }
}
