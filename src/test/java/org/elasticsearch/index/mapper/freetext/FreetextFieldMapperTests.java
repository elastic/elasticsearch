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
package org.elasticsearch.index.mapper.freetext;

import org.apache.lucene.search.suggest.analyzing.XFreeTextSuggester;
import org.apache.lucene.search.suggest.analyzing.XFuzzySuggester;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.elasticsearch.index.mapper.core.FreetextFieldMapper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class FreetextFieldMapperTests extends ElasticsearchTestCase {

    @Test
    public void testDefaultConfiguration() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("freetext")
                .field("type", "freetext")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        FieldMapper fieldMapper = defaultMapper.mappers().name("freetext").mapper();
        assertThat(fieldMapper, instanceOf(FreetextFieldMapper.class));

        FreetextFieldMapper freetextFieldMapper = (FreetextFieldMapper) fieldMapper;
        assertThat(freetextFieldMapper.getGram(), is(FreetextFieldMapper.Gram.BIGRAM));
        assertThat(freetextFieldMapper.getSeparator(), is(XFreeTextSuggester.DEFAULT_SEPARATOR));
    }

}
