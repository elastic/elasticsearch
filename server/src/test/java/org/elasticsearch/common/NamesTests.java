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

package org.elasticsearch.common;

import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.function.BiFunction;

public class NamesTests extends ESTestCase {

    private BiFunction<String, String, ? extends RuntimeException> exceptionCtor;

    @Before
    public void setExceptionCtor() {
        exceptionCtor = randomFrom(InvalidIndexNameException::new,
            InvalidAliasNameException::new,
            (name, msg) -> new IllegalArgumentException(name + " is invalid: " + msg));
    }

    public void testValidateTopLevelName() {
        for (Character badChar : Strings.INVALID_FILENAME_CHARS) {
            expectThrows(RuntimeException.class, () -> Names.validateNameWithIndexNameRules(randomAlphaOfLengthBetween(0, 10) +
                badChar + randomAlphaOfLengthBetween(0, 10), exceptionCtor));
        }
        expectThrows(RuntimeException.class, () -> Names.validateNameWithIndexNameRules(randomAlphaOfLengthBetween(0, 10) +
            "#" + randomAlphaOfLengthBetween(0, 10), exceptionCtor));
        expectThrows(RuntimeException.class, () -> Names.validateNameWithIndexNameRules(randomAlphaOfLengthBetween(0, 10) +
            ":" + randomAlphaOfLengthBetween(0, 10), exceptionCtor));
        expectThrows(RuntimeException.class, () -> Names.validateNameWithIndexNameRules("_" + randomAlphaOfLengthBetween(1, 20),
            exceptionCtor));
        expectThrows(RuntimeException.class, () -> Names.validateNameWithIndexNameRules("-" + randomAlphaOfLengthBetween(1, 20),
            exceptionCtor));
        expectThrows(RuntimeException.class, () -> Names.validateNameWithIndexNameRules("+" + randomAlphaOfLengthBetween(1, 20),
            exceptionCtor));
        expectThrows(RuntimeException.class, () -> Names.validateNameWithIndexNameRules(".", exceptionCtor));
        expectThrows(RuntimeException.class, () -> Names.validateNameWithIndexNameRules("..", exceptionCtor));
        expectThrows(RuntimeException.class, () -> Names.validateNameWithIndexNameRules(randomAlphaOfLengthBetween(256, 1000),
            exceptionCtor));

        Names.validateNameWithIndexNameRules(randomAlphaOfLengthBetween(1, 10) + "_" + randomAlphaOfLengthBetween(0, 10), exceptionCtor);
        Names.validateNameWithIndexNameRules(randomAlphaOfLengthBetween(1, 10) + "-" + randomAlphaOfLengthBetween(0, 10), exceptionCtor);
        Names.validateNameWithIndexNameRules(randomAlphaOfLengthBetween(1, 10) + "+" + randomAlphaOfLengthBetween(0, 10), exceptionCtor);

        Names.validateNameWithIndexNameRules(randomAlphaOfLengthBetween(0, 10) + "." + randomAlphaOfLengthBetween(1, 10), exceptionCtor);
        Names.validateNameWithIndexNameRules(randomAlphaOfLengthBetween(0, 10) + ".." + randomAlphaOfLengthBetween(1, 10), exceptionCtor);

        Names.validateNameWithIndexNameRules(randomAlphaOfLengthBetween(1, 255), exceptionCtor);
    }
}
