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

package org.elasticsearch.test.unit.index.mapper.merge;

import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.test.unit.index.mapper.MapperTests;
import org.testng.annotations.Test;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.index.mapper.DocumentMapper.MergeFlags.mergeFlags;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@Test
public class Test1MergeMapperTests {

    @Test
    public void test1Merge() throws Exception {
        String stage1Mapping = copyToStringFromClasspath("/org/elasticsearch/test/unit/index/mapper/merge/stage1.json");
        DocumentMapper stage1 = MapperTests.newParser().parse(stage1Mapping);
        String stage2Mapping = copyToStringFromClasspath("/org/elasticsearch/test/unit/index/mapper/merge/stage2.json");
        DocumentMapper stage2 = MapperTests.newParser().parse(stage2Mapping);

        DocumentMapper.MergeResult mergeResult = stage1.merge(stage2, mergeFlags().simulate(true));
        assertThat(mergeResult.hasConflicts(), equalTo(false));
        // since we are simulating, we should not have the age mapping
        assertThat(stage1.mappers().smartName("age"), nullValue());
        assertThat(stage1.mappers().smartName("obj1.prop1"), nullValue());
        // now merge, don't simulate
        mergeResult = stage1.merge(stage2, mergeFlags().simulate(false));
        // there is still merge failures
        assertThat(mergeResult.hasConflicts(), equalTo(false));
        // but we have the age in
        assertThat(stage1.mappers().smartName("age"), notNullValue());
        assertThat(stage1.mappers().smartName("obj1.prop1"), notNullValue());
    }
}
