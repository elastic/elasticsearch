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

package org.elasticsearch.index.mapper.typelevels;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class ParseMappingTypeLevelTests extends ElasticsearchTestCase {

    @Test
    public void testTypeLevel() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = MapperTestUtils.newParser().parse("type", mapping);
        assertThat(mapper.type(), equalTo("type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(false));

        mapper = MapperTestUtils.newParser().parse(mapping);
        assertThat(mapper.type(), equalTo("type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(false));
    }
}
