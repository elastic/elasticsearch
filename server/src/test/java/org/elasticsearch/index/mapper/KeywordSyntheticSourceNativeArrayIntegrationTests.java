/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class KeywordSyntheticSourceNativeArrayIntegrationTests extends NativeArrayIntegrationTestCase {

    @Override
    protected String getFieldTypeName() {
        return "keyword";
    }

    @Override
    protected String getRandomValue() {
        return RandomStrings.randomAsciiOfLength(random(), 8);
    }

    @Override
    public Object getMalformedValue() {
        return null;
    }

    public void testSynthesizeArray() throws Exception {
        var arrayValues = new Object[][] {
            new Object[] { "z", "y", null, "x", null, "v" },
            new Object[] { null, "b", null, "a" },
            new Object[] { null },
            new Object[] { null, null, null },
            new Object[] { "c", "b", "a" } };
        verifySyntheticArray(arrayValues);
    }

    public void testSynthesizeArrayIgnoreAbove() throws Exception {
        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "keyword")
            .field("ignore_above", 4)
            .endObject()
            .endObject()
            .endObject();
        // Note values that would be ignored are added at the end of arrays,
        // this makes testing easier as ignored values are always synthesized after regular values:
        var arrayValues = new Object[][] {
            new Object[] { null, "a", "ab", "abc", "abcd", null, "abcde" },
            new Object[] { "12345", "12345", "12345" },
            new Object[] { "123", "1234", "12345" },
            new Object[] { null, null, null, "blabla" },
            new Object[] { "1", "2", "3", "blabla" } };
        verifySyntheticArray(arrayValues, mapping, "_id", "field._original");
    }

    public void testSynthesizeObjectArray() throws Exception {
        List<List<Object[]>> documents = new ArrayList<>();
        {
            List<Object[]> document = new ArrayList<>();
            document.add(new Object[] { "z", "y", "x" });
            document.add(new Object[] { "m", "l", "m" });
            document.add(new Object[] { "c", "b", "a" });
            documents.add(document);
        }
        {
            List<Object[]> document = new ArrayList<>();
            document.add(new Object[] { "9", "7", "5" });
            document.add(new Object[] { "2", "4", "6" });
            document.add(new Object[] { "7", "6", "5" });
            documents.add(document);
        }
        verifySyntheticObjectArray(documents);
    }

    public void testSynthesizeArrayInObjectField() throws Exception {
        List<Object[]> documents = new ArrayList<>();
        documents.add(new Object[] { "z", "y", "x" });
        documents.add(new Object[] { "m", "l", "m" });
        documents.add(new Object[] { "c", "b", "a" });
        documents.add(new Object[] { "9", "7", "5" });
        documents.add(new Object[] { "2", "4", "6" });
        documents.add(new Object[] { "7", "6", "5" });
        verifySyntheticArrayInObject(documents);
    }

}
