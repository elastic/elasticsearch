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

package org.elasticsearch.index.mapper.meta;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class MapperMetaTests extends ElasticsearchTestCase {

    @Test
    public void testMeta() throws IOException {
        final String TEST_ATTR1 = "testattr1";
        final String TEST_VALUE = "testvalue";
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/meta/test-mapping.json");
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);

        // test document meta
        assertThat((String) docMapper.meta().get(TEST_ATTR1), equalTo(TEST_VALUE));

        // test root mapper mata
        assertThat((String) docMapper.idFieldMapper().meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.parentFieldMapper().meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.sourceMapper().meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.allFieldMapper().meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.timestampFieldMapper().meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.SizeFieldMapper().meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.TTLFieldMapper().meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.IndexFieldMapper().meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.routingFieldMapper().meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.boostFieldMapper().meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.analyzerMapper().meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.uidMapper().meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.typeMapper().meta().get(TEST_ATTR1), equalTo(TEST_VALUE));

        // test field mapper meta
        assertThat((String) docMapper.mappers().smartNameFieldMapper("string").meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.mappers().smartNameFieldMapper("binary").meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.mappers().smartNameFieldMapper("token_count").meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.mappers().smartNameFieldMapper("date").meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.mappers().smartNameFieldMapper("boolean").meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.mappers().smartNameFieldMapper("integer").meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.mappers().smartNameFieldMapper("completion").meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.mappers().smartNameFieldMapper("ip").meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.mappers().smartNameFieldMapper("geo_point").meta().get(TEST_ATTR1), equalTo(TEST_VALUE));
        assertThat((String) docMapper.mappers().smartNameFieldMapper("geo_shape").meta().get(TEST_ATTR1), equalTo(TEST_VALUE));

        // test convert back from mapper
        int metaCount = StringUtils.countMatches(mapping, TEST_ATTR1);
        XContentBuilder builder = jsonBuilder();
        docMapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String xmapping = builder.string();
        assertThat(StringUtils.countMatches(xmapping, TEST_ATTR1), equalTo(metaCount));

        // test merge meta
        final String TEST_ATTR2 = "testattr2";
        String mapping2 = mapping.replaceAll(TEST_ATTR1, TEST_ATTR2);
        DocumentMapper docMapper2 = MapperTestUtils.newParser().parse(mapping2);
        docMapper.merge(docMapper2, DocumentMapper.MergeFlags.mergeFlags().simulate(false));

        XContentBuilder builder2 = jsonBuilder();
        docMapper.toXContent(builder2, ToXContent.EMPTY_PARAMS);
        String xmapping2 = builder2.string();
        assertThat(StringUtils.countMatches(xmapping2, TEST_ATTR2), equalTo(metaCount));
        assertThat(StringUtils.countMatches(xmapping2, TEST_ATTR1), equalTo(0));
    }


}
