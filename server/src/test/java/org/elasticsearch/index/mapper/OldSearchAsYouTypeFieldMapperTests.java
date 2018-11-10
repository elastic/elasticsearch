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

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.OldSearchAsYouTypeFieldMapper.SearchAsYouTypeAnalyzer;
import org.elasticsearch.index.mapper.OldSearchAsYouTypeFieldMapper.SearchAsYouTypeFieldType;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class OldSearchAsYouTypeFieldMapperTests extends ESSingleNodeTestCase {

    public void testDefaultConfiguration() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("a_field")
            .field("type", "search_as_you_type")
            .endObject()
            .endObject()
            .endObject()
            .endObject());

        DocumentMapper defaultMapper = createIndex("test")
            .mapperService()
            .documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper("a_field");
        assertThat(fieldMapper, instanceOf(OldSearchAsYouTypeFieldMapper.class));
        OldSearchAsYouTypeFieldMapper oldSearchAsYouTypeFieldMapper = (OldSearchAsYouTypeFieldMapper) fieldMapper;

        SearchAsYouTypeFieldType fieldType = oldSearchAsYouTypeFieldMapper.fieldType();
        NamedAnalyzer indexAnalyzer = fieldType.indexAnalyzer(); // true
        NamedAnalyzer searchAnalyzer = fieldType.searchAnalyzer(); // false

        for (NamedAnalyzer analyzer : Arrays.asList(indexAnalyzer, searchAnalyzer)) {
            assertThat(analyzer.name(), equalTo("default"));
            assertThat(analyzer.analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
        }

        SearchAsYouTypeAnalyzer indexSearchAsYouTypeAnalyzer = (SearchAsYouTypeAnalyzer) indexAnalyzer.analyzer();
        assertThat(indexSearchAsYouTypeAnalyzer.isWithEdgeNgram(), equalTo(true));

        SearchAsYouTypeAnalyzer searchSearchAsYouTypeAnalyzer = (SearchAsYouTypeAnalyzer) searchAnalyzer.analyzer();
        assertThat(searchSearchAsYouTypeAnalyzer.isWithEdgeNgram(), equalTo(false));
    }

}
