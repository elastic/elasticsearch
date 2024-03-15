/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.hamcrest.core.IsEqual.equalTo;

@ESTestCase.WithoutSecurityManager
public class VectorDataInputTests extends AbstractVectorTestCase {

    public void testFloatSimple() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        Path topLevelDir = createTempDir(getTestName());
        var factory = NativeAccess.instance().getVectorScorerFactory();

        for (int times = 0; times < 10; times++) {
            int dims = randomIntBetween(1, 10);
            var pad = new byte[dims];
            byte[] bytes = concat(
                pad,
                floatToByteArray(0.0f),
                pad,
                floatToByteArray(1.1f),
                pad,
                floatToByteArray(Float.MAX_VALUE),
                pad,
                floatToByteArray(Float.MIN_VALUE)
            );
            Path path = topLevelDir.resolve("data-" + times + ".vec");
            Files.write(path, bytes, CREATE_NEW);
            assertThat(Files.size(path), equalTo(4L * (dims + Float.BYTES)));

            // TODO: move to unit tests inside jdk impl?
            /*try (var scorer = factory.getScalarQuantizedVectorScorer(dims, 4, 1, DOT_PRODUCT, path)) {
                VectorDataAccessor accessor = new VectorDataAccessor(scorer);
                float f = accessor.getFloat(1);
                assertThat(f, equalTo(0.0f));
                f = accessor.getFloat(dims + Float.BYTES + dims);
                assertThat(f, equalTo(1.1f));
                f = accessor.getFloat(dims + Float.BYTES + dims + Float.BYTES + dims);
                assertThat(f, equalTo(Float.MAX_VALUE));
                f = accessor.getFloat(dims + Float.BYTES + dims + Float.BYTES + dims + Float.BYTES + dims);
                assertThat(f, equalTo(Float.MIN_VALUE));
            }*/
        }
    }

    // Access internals - whitebox test
    static class VectorDataAccessor {

        static final VarHandle getDataHandle;
        static final MethodHandle getFloatHandle;

        // static final MethodHandle getAddressHandle;

        static {
            try {
                Class<?> scorerCls = Class.forName("org.elasticsearch.vec.internal.AbstractScalarQuantizedVectorScorer");
                Class<?> dataCls = Class.forName("org.elasticsearch.vec.internal.VectorDataInput");

                var lookup = MethodHandles.lookup();
                getDataHandle = MethodHandles.privateLookupIn(scorerCls, lookup).findVarHandle(scorerCls, "data", dataCls);
                var mt = MethodType.methodType(float.class, long.class);
                getFloatHandle = MethodHandles.privateLookupIn(dataCls, lookup).findVirtual(dataCls, "readFloat", mt);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }

        private final Object scorer;

        VectorDataAccessor(VectorScorer scorer) {
            this.scorer = scorer;
        }

        float getFloat(long address) {
            try {
                return (float) getFloatHandle.invoke(getDataHandle.get(scorer), address);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }
    }
}
