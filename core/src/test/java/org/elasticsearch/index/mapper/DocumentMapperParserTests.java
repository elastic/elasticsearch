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

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class DocumentMapperParserTests extends ESTestCase {

    public void testRemoveDotsInFieldNames() {
        Map<String, Object> withDots = new MapBuilder<String, Object>()
                .put("properties", new MapBuilder<String, Object>()
                        .put("a.b", new MapBuilder<String, Object>()
                                .put("type", "float")
                                .map())
                        .map())
                .map();
        Map<String, Object> withoutDots = new MapBuilder<String, Object>()
                .put("properties", new MapBuilder<String, Object>()
                        .put("a", new MapBuilder<String, Object>()
                                .put("properties", new MapBuilder<String, Object>()
                                        .put("b", new MapBuilder<String, Object>()
                                                .put("type", "float")
                                                .map())
                                        .map())
                                .map())
                        .map())
                .map();
        assertEquals(withoutDots, DocumentMapperParser.removeDotsInFieldNames(withDots));
    }

    public void testRemoveDotsInFieldNames2Levels() {
        Map<String, Object> withDots = new MapBuilder<String, Object>()
                .put("properties", new MapBuilder<String, Object>()
                        .put("a.b.c", new MapBuilder<String, Object>()
                                .put("type", "float")
                                .map())
                        .map())
                .map();
        Map<String, Object> withoutDots = new MapBuilder<String, Object>()
                .put("properties", new MapBuilder<String, Object>()
                        .put("a", new MapBuilder<String, Object>()
                                .put("properties", new MapBuilder<String, Object>()
                                        .put("b", new MapBuilder<String, Object>()
                                                .put("properties", new MapBuilder<String, Object>()
                                                        .put("c", new MapBuilder<String, Object>()
                                                                .put("type", "float")
                                                                .map())
                                                        .map())
                                                .map())
                                        .map())
                                .map())
                        .map())
                .map();
        assertEquals(withoutDots, DocumentMapperParser.removeDotsInFieldNames(withDots));
    }

    public void testRemoveDotsInFieldNamesInInnerObject() {
        Map<String, Object> withDots = new MapBuilder<String, Object>()
                .put("properties", new MapBuilder<String, Object>()
                        .put("a", new MapBuilder<String, Object>()
                                .put("properties", new MapBuilder<String, Object>()
                                        .put("b.c", new MapBuilder<String, Object>()
                                                .put("type", "float")
                                                .map())
                                        .map())
                                .map())
                        .map())
                .map();
        Map<String, Object> withoutDots = new MapBuilder<String, Object>()
                .put("properties", new MapBuilder<String, Object>()
                        .put("a", new MapBuilder<String, Object>()
                                .put("properties", new MapBuilder<String, Object>()
                                        .put("b", new MapBuilder<String, Object>()
                                                .put("properties", new MapBuilder<String, Object>()
                                                        .put("c", new MapBuilder<String, Object>()
                                                                .put("type", "float")
                                                                .map())
                                                        .map())
                                                .map())
                                        .map())
                                .map())
                        .map())
                .map();
        assertEquals(withoutDots, DocumentMapperParser.removeDotsInFieldNames(withDots));
    }

    public void testRemoveDotsInFieldNamesMerge() {
        Map<String, Object> withDots = new MapBuilder<String, Object>()
                .put("properties", new MapBuilder<String, Object>()
                        .put("a.b", new MapBuilder<String, Object>()
                                .put("type", "float")
                                .map())
                        .put("a", new MapBuilder<String, Object>()
                                .put("properties", new MapBuilder<String, Object>()
                                        .put("c", new MapBuilder<String, Object>()
                                                .put("type", "integer")
                                                .map())
                                        .map())
                                .map())
                        .map())
                .map();
        Map<String, Object> withoutDots = new MapBuilder<String, Object>()
                .put("properties", new MapBuilder<String, Object>()
                        .put("a", new MapBuilder<String, Object>()
                                .put("properties", new MapBuilder<String, Object>()
                                        .put("c", new MapBuilder<String, Object>()
                                                .put("type", "integer")
                                                .map())
                                        .put("b", new MapBuilder<String, Object>()
                                                .put("type", "float")
                                                .map())
                                        .map())
                                .map())
                        .map())
                .map();
        assertEquals(withoutDots, DocumentMapperParser.removeDotsInFieldNames(withDots));
    }

    public void testRemoveDotsInFieldNamesConflict1() {
        Map<String, Object> withDots = new MapBuilder<String, Object>()
                .put("properties", new MapBuilder<String, Object>()
                        .put("a.b", new MapBuilder<String, Object>()
                                .put("type", "float")
                                .map())
                        .put("a", new MapBuilder<String, Object>()
                                .put("properties", new MapBuilder<String, Object>()
                                        .put("b", new MapBuilder<String, Object>()
                                                .put("type", "integer")
                                                .map())
                                        .map())
                                .map())
                        .map())
                .map();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> DocumentMapperParser.removeDotsInFieldNames(withDots));
        assertEquals("Mapping contains two definitions for field [a.b]", e.getMessage());
    }

    public void testRemoveDotsInFieldNamesConflict2() {
        Map<String, Object> withDots = new MapBuilder<String, Object>()
                .put("properties", new MapBuilder<String, Object>()
                        .put("a.b", new MapBuilder<String, Object>()
                                .put("type", "float")
                                .map())
                        .put("a", new MapBuilder<String, Object>()
                                .put("type", "integer")
                                .map())
                        .map())
                .map();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> DocumentMapperParser.removeDotsInFieldNames(withDots));
        assertEquals("Need to create an object mapping called [a] for field [a.b] but this field already exists and is a [integer]",
                e.getMessage());
    }
}
