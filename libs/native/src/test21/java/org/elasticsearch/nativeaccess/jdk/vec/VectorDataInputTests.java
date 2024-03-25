/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk.vec;

import org.apache.lucene.util.Unwrappable;
import org.elasticsearch.nativeaccess.AbstractVectorTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.hamcrest.core.IsEqual.equalTo;

public class VectorDataInputTests extends AbstractVectorTestCase {

    public void testFloatSimple() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        Path topLevelDir = createTempDir(getTestName());

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
            path = Unwrappable.unwrapAll(path);

            try (var input = VectorDataInput.createVectorDataInput(path);) {
                float f = input.readFloat(1);
                assertThat(f, equalTo(0.0f));
                f = input.readFloat(dims + Float.BYTES + dims);
                assertThat(f, equalTo(1.1f));
                f = input.readFloat(dims + Float.BYTES + dims + Float.BYTES + dims);
                assertThat(f, equalTo(Float.MAX_VALUE));
                f = input.readFloat(dims + Float.BYTES + dims + Float.BYTES + dims + Float.BYTES + dims);
                assertThat(f, equalTo(Float.MIN_VALUE));
            }
        }
    }
}
