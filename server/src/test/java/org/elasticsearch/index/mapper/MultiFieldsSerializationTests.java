/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.script.ScriptCompiler;
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
            builder.add(new BooleanFieldMapper.Builder(name, ScriptCompiler.NONE, false, IndexVersion.CURRENT));
        }

        Mapper.Builder root = new BooleanFieldMapper.Builder("root", ScriptCompiler.NONE, false, IndexVersion.CURRENT);
        FieldMapper.MultiFields multiFields = builder.build(root, MapperBuilderContext.root(false));

        String serialized = Strings.toString(multiFields);
        int lastStart = 0;
        for (String name : sortedNames) {
            int pos = serialized.indexOf(name);
            assertThat(pos, greaterThan(lastStart));
            lastStart = pos;
        }
    }

}
