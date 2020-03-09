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

package org.elasticsearch.search;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.LuceneTests;
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
        Object[] sortValues = instance.getFormattedSortValues();
        Object[] newValues = Arrays.copyOf(sortValues, sortValues.length + 1);
        DocValueFormat[] newFormats = Arrays.copyOf(instance.getSortValueFormats(), sortValues.length + 1);
        newValues[sortValues.length] =  LuceneTests.randomSortValue();
        newFormats[sortValues.length] = DocValueFormat.RAW;
        return new SearchSortValuesAndFormats(newValues, newFormats);
    }

    public static SearchSortValuesAndFormats randomInstance()  {
        int size = randomIntBetween(1, 20);
        Object[] values = new Object[size];
        DocValueFormat[] sortValueFormats = new DocValueFormat[size];
        for (int i = 0; i < size; i++) {
            values[i] = LuceneTests.randomSortValue();
            sortValueFormats[i] = DocValueFormat.RAW;
        }
        return new SearchSortValuesAndFormats(values, sortValueFormats);
    }
}
