/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

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

    public void testUnformattableLongSortValueThrowsIllegalArgument() {
        DocValueFormat noNumericFormat = new DocValueFormat() {
            @Override
            public String getWriteableName() {
                return "test_no_numeric_format";
            }

            @Override
            public void writeTo(StreamOutput out) {}
            // format(long) and format(double) are not overridden, inheriting the UnsupportedOperationException defaults
        };
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SearchSortValuesAndFormats(new Object[] { randomLong() }, new DocValueFormat[] { noNumericFormat })
        );
        assertThat(e.getMessage(), containsString("test_no_numeric_format"));
        assertThat(e.getMessage(), containsString("does not support sorting"));
    }

    public void testUnformattableDoubleSortValueThrowsIllegalArgument() {
        DocValueFormat noNumericFormat = new DocValueFormat() {
            @Override
            public String getWriteableName() {
                return "test_no_numeric_format";
            }

            @Override
            public void writeTo(StreamOutput out) {}
        };
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SearchSortValuesAndFormats(new Object[] { randomDouble() }, new DocValueFormat[] { noNumericFormat })
        );
        assertThat(e.getMessage(), containsString("test_no_numeric_format"));
        assertThat(e.getMessage(), containsString("does not support sorting"));
    }

    public void testUnformattableBytesSortValueThrowsIllegalArgument() {
        DocValueFormat noBytesFormat = new DocValueFormat() {
            @Override
            public String getWriteableName() {
                return "test_no_bytes_format";
            }

            @Override
            public void writeTo(StreamOutput out) {}
        };
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SearchSortValuesAndFormats(
                new Object[] { new BytesRef(randomAlphaOfLength(5)) },
                new DocValueFormat[] { noBytesFormat }
            )
        );
        assertThat(e.getMessage(), containsString("test_no_bytes_format"));
        assertThat(e.getMessage(), containsString("does not support sorting"));
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
