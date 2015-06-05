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

package org.elasticsearch.index.mapper.path;

import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class PathMapperTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testPathMapping() throws IOException {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/path/test-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        // test full name
        assertThat(docMapper.mappers().getMapper("first1"), nullValue());
        assertThat(docMapper.mappers().getMapper("name1.first1"), notNullValue());
        assertThat(docMapper.mappers().getMapper("last1"), nullValue());
        assertThat(docMapper.mappers().getMapper("i_last_1"), nullValue());
        assertThat(docMapper.mappers().getMapper("name1.last1"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name1.i_last_1"), nullValue());

        assertThat(docMapper.mappers().getMapper("first2"), nullValue());
        assertThat(docMapper.mappers().getMapper("name2.first2"), notNullValue());
        assertThat(docMapper.mappers().getMapper("last2"), nullValue());
        assertThat(docMapper.mappers().getMapper("i_last_2"), nullValue());
        assertThat(docMapper.mappers().getMapper("name2.i_last_2"), nullValue());
        assertThat(docMapper.mappers().getMapper("name2.last2"), notNullValue());
    }
}
