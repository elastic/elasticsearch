/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static java.util.Collections.singletonMap;

public class MatrixFieldProcessorTests extends AbstractWireSerializingTestCase<MatrixFieldProcessor> {
    public static MatrixFieldProcessor randomMatrixFieldProcessor() {
        return new MatrixFieldProcessor(randomAlphaOfLength(5));
    }

    @Override
    protected MatrixFieldProcessor createTestInstance() {
        return randomMatrixFieldProcessor();
    }

    @Override
    protected Reader<MatrixFieldProcessor> instanceReader() {
        return MatrixFieldProcessor::new;
    }

    @Override
    protected MatrixFieldProcessor mutateInstance(MatrixFieldProcessor instance) throws IOException {
        return new MatrixFieldProcessor(randomValueOtherThan(instance.key(), () -> randomAlphaOfLength(5)));
    }

    public void testApply() {
        MatrixFieldProcessor proc = new MatrixFieldProcessor("test");
        assertEquals(null, proc.apply(null));
        assertEquals("cat", proc.apply("cat"));
        assertEquals(null, proc.apply(singletonMap("foo", "cat")));
        assertEquals("cat", proc.apply(singletonMap("test", "cat")));
    }
}
