/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.hasItem;

public abstract class AbstractNumericFieldMapperTestCase extends MapperTestCase {
    protected abstract Set<String> types();
    protected abstract Set<String> wholeTypes();

    public final void testTypesAndWholeTypes() {
        for (String wholeType : wholeTypes()) {
            assertThat(types(), hasItem(wholeType));
        }
    }

    public final void testDefaults() throws Exception {
        for (String type : types()) {
            doTestDefaults(type);
        }
    }

    protected abstract void doTestDefaults(String type) throws Exception;

    public final void testNotIndexed() throws Exception {
        for (String type : types()) {
            doTestNotIndexed(type);
        }
    }

    protected abstract void doTestNotIndexed(String type) throws Exception;

    public final void testNoDocValues() throws Exception {
        for (String type : types()) {
            doTestNoDocValues(type);
        }
    }

    protected abstract void doTestNoDocValues(String type) throws Exception;

    public final void testStore() throws Exception {
        for (String type : types()) {
            doTestStore(type);
        }
    }

    protected abstract void doTestStore(String type) throws Exception;

    public final void testCoerce() throws Exception {
        for (String type : types()) {
            doTestCoerce(type);
        }
    }

    protected abstract void doTestCoerce(String type) throws IOException;

    public final void testDecimalCoerce() throws Exception {
        for (String type : wholeTypes()) {
            doTestDecimalCoerce(type);
        }
    }

    protected abstract void doTestDecimalCoerce(String type) throws IOException;

    public final void testNullValue() throws IOException {
        for (String type : types()) {
            doTestNullValue(type);
        }
    }

    protected abstract void doTestNullValue(String type) throws IOException;

    @Override
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        b.field("type", randomFrom(types()));
    }
}
