/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class MultiFieldsSerializationTests extends ESTestCase {

    public void testSorting() throws IOException {

        FieldMapper.MultiFields.Builder builder = new FieldMapper.MultiFields.Builder();

        Set<String> names = new HashSet<>();
        int count = randomIntBetween(5, 20);
        for (int i = 0; i < count; i++) {
            names.add(randomAlphaOfLengthBetween(5, 10));
        }

        List<String> sortedNames = new ArrayList<>(names);
        sortedNames.sort(Comparator.naturalOrder());

        for (String name : names) {
            builder.add(new BooleanFieldMapper.Builder(name, ScriptCompiler.NONE, defaultIndexSettings()));
        }

        XContentBuilder xContent = XContentFactory.jsonBuilder().startObject();
        builder.toXContent(xContent, ToXContent.EMPTY_PARAMS);
        String serialized = Strings.toString(xContent.endObject());
        int lastStart = 0;
        for (String name : sortedNames) {
            int pos = serialized.indexOf(name);
            assertThat(pos, greaterThan(lastStart));
            lastStart = pos;
        }
    }

}
