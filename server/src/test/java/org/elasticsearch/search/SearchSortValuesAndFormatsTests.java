/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SearchSortValuesAndFormatsTests extends AbstractWireSerializingTestCase<SearchSortValuesAndFormats> {
    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void initRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    protected Writeable.Reader<SearchSortValuesAndFormats> instanceReader() {
        return SearchSortValuesAndFormats::new;
    }

    @Override
    protected SearchSortValuesAndFormats createTestInstance() {
        return randomInstance();
    }

    @Override
    protected SearchSortValuesAndFormats mutateInstance(SearchSortValuesAndFormats instance) {
        Object[] sortValues = instance.getRawSortValues();
        Object[] newValues = Arrays.copyOf(sortValues, sortValues.length + 1);
        DocValueFormat[] newFormats = Arrays.copyOf(instance.getSortValueFormats(), sortValues.length + 1);
        newValues[sortValues.length] = randomSortValue();
        newFormats[sortValues.length] = DocValueFormat.RAW;
        return new SearchSortValuesAndFormats(newValues, newFormats);
    }

    private static Object randomSortValue() {
        return switch (randomIntBetween(0, 5)) {
            case 0 -> null;
            case 1 -> new BytesRef(randomAlphaOfLengthBetween(3, 10));
            case 2 -> randomInt();
            case 3 -> randomLong();
            case 4 -> randomFloat();
            case 5 -> randomDouble();
            default -> throw new UnsupportedOperationException();
        };
    }

    public static SearchSortValuesAndFormats randomInstance() {
        int size = randomIntBetween(1, 20);
        Object[] values = new Object[size];
        DocValueFormat[] sortValueFormats = new DocValueFormat[size];
        for (int i = 0; i < size; i++) {
            values[i] = randomSortValue();
            sortValueFormats[i] = DocValueFormat.RAW;
        }
        return new SearchSortValuesAndFormats(values, sortValueFormats);
    }
}
