/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class MultiFieldsSerializationTests extends ESTestCase {

    public void testSorting() {

        FieldMapper.MultiFields.Builder builder = new FieldMapper.MultiFields.Builder();

        Set<String> names = new HashSet<>();
        int count = randomIntBetween(5, 20);
        for (int i = 0; i < count; i++) {
            names.add(randomAlphaOfLengthBetween(5, 10));
        }

        List<String> sortedNames = new ArrayList<>(names);
        sortedNames.sort(Comparator.naturalOrder());

        for (String name : names) {
            builder.add(new BooleanFieldMapper.Builder(name));
        }

        Mapper.Builder root = new BooleanFieldMapper.Builder("root");
        FieldMapper.MultiFields multiFields = builder.build(root, new ContentPath());

        String serialized = Strings.toString(multiFields);
        int lastStart = 0;
        for (String name : sortedNames) {
            int pos = serialized.indexOf(name);
            assertThat(pos, greaterThan(lastStart));
            lastStart = pos;
        }
    }

}
