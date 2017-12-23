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

package org.elasticsearch.monitor.fs;

import java.util.Arrays;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;

public class FsInfoTests extends ESTestCase {

    public void testFsInfo() {
        final FsInfo.Path[] paths = randomPaths();
        final long timestamp = randomLong();
        FsInfo fsInfo = new FsInfo(timestamp, null, paths);

        long size = Arrays.stream(paths).map(n -> n.getTotal().getBytes()).mapToLong(n -> n).sum();
        assertThat(timestamp, equalTo(fsInfo.getTimestamp()));
        assertThat(size, equalTo(fsInfo.getTotal().getTotal().getBytes()));
    }

    public void testFsInfoSameMount() {
        FsInfo.Path[] paths = randomPaths();
        final String mount = randomAlphaOfLength(10);
        Arrays.stream(paths).forEach(p -> p.mount = mount);
        final long timestamp = randomLong();
        FsInfo fsInfo = new FsInfo(timestamp, null, paths);

        long size = paths[0].getTotal().getBytes();
        assertThat(timestamp, equalTo(fsInfo.getTimestamp()));
        assertThat(size, equalTo(fsInfo.getTotal().getTotal().getBytes()));
    }

    private FsInfo.Path[] randomPaths() {
        final int totalPaths = randomIntBetween(2, 4);
        FsInfo.Path[] paths = new FsInfo.Path[totalPaths];
        for (int i=0; i < totalPaths; i++) {
            paths[i] = new FsInfo.Path(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomIntBetween(1, 1 << 16),
                randomIntBetween(1, 1 << 16),
                randomIntBetween(1, 1 << 16)
            );
        }
        return paths;
    }
}
