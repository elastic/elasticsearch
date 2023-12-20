/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource.Field;
import org.junit.Before;

import java.util.List;

import static java.util.Collections.emptyList;

public class ItemSetMapReduceValueSourceSerializationTests extends AbstractWireSerializingTestCase<Field> {

    private NamedWriteableRegistry namedWriteableRegistry;

    @Override
    protected Reader<Field> instanceReader() {
        return Field::new;
    }

    @Override
    protected Field createTestInstance() {
        switch (randomIntBetween(0, 2)) {
            case 0:
                return ItemSetMapReduceValueSourceTests.createKeywordFieldTestInstance(randomAlphaOfLengthBetween(3, 20), randomInt());
            case 1:
                return ItemSetMapReduceValueSourceTests.createIpFieldTestInstance(randomAlphaOfLengthBetween(3, 20), randomInt());
            case 2:
                return ItemSetMapReduceValueSourceTests.createLongFieldTestInstance(randomAlphaOfLengthBetween(3, 20), randomInt());
        }
        throw new AssertionError("field type missing");
    }

    @Override
    protected Field mutateInstance(Field instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

}
