/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.mapper.json.merge.test1;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.json.JsonDocumentMapper;
import org.elasticsearch.index.mapper.json.JsonDocumentMapperParser;
import org.testng.annotations.Test;

import static org.elasticsearch.index.mapper.DocumentMapper.MergeFlags.*;
import static org.elasticsearch.util.io.Streams.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class Test1MergeMapperTests {

    @Test public void test1Merge() throws Exception {
        String stage1Mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/json/merge/test1/stage1.json");
        JsonDocumentMapper stage1 = (JsonDocumentMapper) new JsonDocumentMapperParser(new AnalysisService(new Index("test"))).parse(stage1Mapping);
        String stage2Mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/json/merge/test1/stage2.json");
        JsonDocumentMapper stage2 = (JsonDocumentMapper) new JsonDocumentMapperParser(new AnalysisService(new Index("test"))).parse(stage2Mapping);
        try {
            stage1.merge(stage2, mergeFlags().simulate(true));
            assert false : "can't change field from number to type";
        } catch (MergeMappingException e) {
            // all is well
        }

        // now, test with ignore duplicates
        stage1.merge(stage2, mergeFlags().ignoreDuplicates(true).simulate(true));
        // since we are simulating, we should not have the age mapping
        assertThat(stage1.mappers().smartName("age"), nullValue());
        // now merge, ignore duplicates and don't simulate
        stage1.merge(stage2, mergeFlags().ignoreDuplicates(true).simulate(false));
        assertThat(stage1.mappers().smartName("age"), notNullValue());
    }
}
