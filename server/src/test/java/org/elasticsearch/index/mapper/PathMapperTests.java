/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.io.IOException;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PathMapperTests extends MapperServiceTestCase {
    public void testPathMapping() throws IOException {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/path/test-mapping.json");
        DocumentMapper docMapper = createDocumentMapper(mapping);

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
