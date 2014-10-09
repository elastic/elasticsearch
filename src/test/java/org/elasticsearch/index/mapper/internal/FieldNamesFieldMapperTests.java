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

package org.elasticsearch.index.mapper.internal;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FieldNamesFieldMapperTests extends ElasticsearchSingleNodeTest {

    private static Set<String> extract(String path) {
        return ImmutableSet.<String>builder().addAll(FieldNamesFieldMapper.extractFieldNames(path)).build();
    }

    private static <T> Set<T> set(T... values) {
        return new HashSet<T>(Arrays.asList(values));
    }

    public void testExtractFieldNames() {
        assertEquals(set("abc"), extract("abc"));
        assertEquals(set("a", "a.b"), extract("a.b"));
        assertEquals(set("a", "a.b", "a.b.c"), extract("a.b.c"));
        // and now corner cases
        assertEquals(set("", ".a"), extract(".a"));
        assertEquals(set("a", "a."), extract("a."));
        assertEquals(set("", ".", ".."), extract(".."));
    }

    public void test() throws Exception {
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string());

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .field("a", "100")
                    .startObject("b")
                        .field("c", 42)
                    .endObject()
                .endObject()
                .bytes());

        final Set<String> fieldNames = new HashSet<>();
        for (IndexableField field : doc.rootDoc().getFields()) {
            if (FieldNamesFieldMapper.CONTENT_TYPE.equals(field.name())) {
                fieldNames.add(field.stringValue());
            }
        }
        assertEquals(new HashSet<>(Arrays.asList("a", "b", "b.c", "_uid", "_type", "_version", "_source", "_all")), fieldNames);
    }
}
