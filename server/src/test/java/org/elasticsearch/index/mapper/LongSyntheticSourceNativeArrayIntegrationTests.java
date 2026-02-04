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

public class LongSyntheticSourceNativeArrayIntegrationTests extends NativeArrayIntegrationTestCase {

    @Override
    protected String getFieldTypeName() {
        return "long";
    }

    @Override
    public Long getRandomValue() {
        return randomLong();
    }

    @Override
    public String getMalformedValue() {
        return RandomStrings.randomAsciiOfLength(random(), 8);
    }

    public void testSynthesizeArray() throws Exception {
        var arrayValues = new Object[][] {
            new Object[] { 26, null, 25, null, 24, null, 22 },
            new Object[] { null, 2, null, -1 },
            new Object[] { null },
            new Object[] { null, null, null },
            new Object[] { 3, 2, 1 },
            new Object[] { 1 },
            new Object[] { 3, 3, 3, 3, 3 } };
        verifySyntheticArray(arrayValues);
    }

    public void testSynthesizeObjectArray() throws Exception {
        List<List<Object[]>> documents = new ArrayList<>();
        {
            List<Object[]> document = new ArrayList<>();
            document.add(new Object[] { 26, 25, 24 });
            document.add(new Object[] { 13, 12, 13 });
            document.add(new Object[] { 3, 2, 1 });
            documents.add(document);
        }
        {
            List<Object[]> document = new ArrayList<>();
            document.add(new Object[] { -12, 14, 6 });
            document.add(new Object[] { 1 });
            document.add(new Object[] { -200, 4 });
            documents.add(document);
        }
        verifySyntheticObjectArray(documents);
    }

    public void testSynthesizeArrayInObjectField() throws Exception {
        List<Object[]> documents = new ArrayList<>();
        documents.add(new Object[] { 26, 25, 24 });
        documents.add(new Object[] { 13, 12, 13 });
        documents.add(new Object[] { 3, 2, 1 });
        documents.add(new Object[] { -20, 5, 7 });
        documents.add(new Object[] { 8, 8, 8 });
        documents.add(new Object[] { 7, 6, 5 });
        verifySyntheticArrayInObject(documents);
    }
}
