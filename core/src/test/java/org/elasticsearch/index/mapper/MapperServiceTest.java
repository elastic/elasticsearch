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

import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class MapperServiceTest extends ElasticsearchSingleNodeTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testTypeNameStartsWithIllegalDot() {
        expectedException.expect(MapperParsingException.class);
        expectedException.expect(hasToString(containsString("mapping type name [.test-type] must not start with a '.'")));
        String index = "test-index";
        String type = ".test-type";
        String field = "field";
        client()
                .admin()
                .indices()
                .prepareCreate(index)
                .addMapping(type, field, "type=string")
                .execute()
                .actionGet();
    }
}
