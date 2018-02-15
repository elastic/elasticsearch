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

package org.elasticsearch;

import org.apache.commons.codec.DecoderException;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.elasticsearch.ExceptionsHelper.MAX_ITERATIONS;
import static org.elasticsearch.ExceptionsHelper.maybeError;
import static org.hamcrest.CoreMatchers.equalTo;

public class ExceptionsHelperTests extends ESTestCase {

    public void testMaybeError() {
        final Error outOfMemoryError = new OutOfMemoryError();
        assertError(outOfMemoryError, outOfMemoryError);

        final DecoderException decoderException = new DecoderException(outOfMemoryError);
        assertError(decoderException, outOfMemoryError);

        final Exception e = new Exception();
        e.addSuppressed(decoderException);
        assertError(e, outOfMemoryError);

        final int depth = randomIntBetween(1, 16);
        Throwable cause = new Exception();
        boolean fatal = false;
        Error error = null;
        for (int i = 0; i < depth; i++) {
            final int length = randomIntBetween(1, 4);
            for (int j = 0; j < length; j++) {
                if (!fatal && rarely()) {
                    error = new Error();
                    cause.addSuppressed(error);
                    fatal = true;
                } else {
                    cause.addSuppressed(new Exception());
                }
            }
            if (!fatal && rarely()) {
                cause = error = new Error(cause);
                fatal = true;
            } else {
                cause = new Exception(cause);
            }
        }
        if (fatal) {
            assertError(cause, error);
        } else {
            assertFalse(maybeError(cause, logger).isPresent());
        }

        assertFalse(maybeError(new Exception(new DecoderException()), logger).isPresent());

        Throwable chain = outOfMemoryError;
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            chain = new Exception(chain);
        }
        assertFalse(maybeError(chain, logger).isPresent());
    }

    private void assertError(final Throwable cause, final Error error) {
        final Optional<Error> maybeError = maybeError(cause, logger);
        assertTrue(maybeError.isPresent());
        assertThat(maybeError.get(), equalTo(error));
    }

}
