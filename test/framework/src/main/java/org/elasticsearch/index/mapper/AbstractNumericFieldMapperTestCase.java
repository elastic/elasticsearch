/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.HalfFloatPoint;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Set;
import java.util.function.Supplier;

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

    protected Supplier<Number> randomFetchValueVendor(NumericType nt) {
        switch (nt) {
            case BYTE:
                return ESTestCase::randomByte;
            case SHORT:
                return ESTestCase::randomShort;
            case INT:
                return ESTestCase::randomInt;
            case LONG:
                return ESTestCase::randomLong;
            case HALF_FLOAT:
                /*
                 * The native valueFetcher returns 32 bits of precision but the
                 * doc values fetcher returns 16 bits of precision. To make it
                 * all line up we round 
                 */
                // https://github.com/elastic/elasticsearch/issues/70260
                return () -> HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(randomFloat()));
            case FLOAT:
                /*
                 * The source parser and doc values round trip will both reduce
                 * the precision to 32 bits if the value is more precise.
                 */
                return randomBoolean() ? () -> randomDoubleBetween(-Float.MAX_VALUE, Float.MAX_VALUE, true) : ESTestCase::randomFloat;
            case DOUBLE:
                /*
                 * The source parser and doc values round trip will both increase
                 * the precision to 64 bits if the value is less precise.
                 */
                return randomBoolean() ? () -> randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true) : ESTestCase::randomFloat;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
