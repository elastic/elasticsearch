/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.type.DataTypeConversion.Conversion;

import java.io.IOException;

public class CastProcessorTests extends AbstractWireSerializingTestCase<CastProcessor> {
    public static CastProcessor randomCastProcessor() {
        return new CastProcessor(randomFrom(Conversion.values()));
    }

    @Override
    protected CastProcessor createTestInstance() {
        return randomCastProcessor();
    }

    @Override
    protected Reader<CastProcessor> instanceReader() {
        return CastProcessor::new;
    }

    @Override
    protected CastProcessor mutateInstance(CastProcessor instance) throws IOException {
        return new CastProcessor(randomValueOtherThan(instance.converter(), () -> randomFrom(Conversion.values())));
    }

    public void testApply() {
        {
            CastProcessor proc = new CastProcessor(Conversion.STRING_TO_INT);
            assertEquals(null, proc.process(null));
            assertEquals(1, proc.process("1"));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> proc.process("1.2"));
            assertEquals("cannot cast [1.2] to [integer]", e.getMessage());
        }
        {
            CastProcessor proc = new CastProcessor(Conversion.BOOL_TO_INT);
            assertEquals(null, proc.process(null));
            assertEquals(1, proc.process(true));
            assertEquals(0, proc.process(false));
        }
    }
    
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }
}
