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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.startsWith;

public class AliasValidatorTests extends ESTestCase {
    public void testValidatesAliasNames() {
        AliasValidator validator = new AliasValidator(Settings.EMPTY);
        Exception e = expectThrows(InvalidAliasNameException.class, () -> validator.validateAliasStandalone(".", null));
        assertEquals("Invalid alias name [.]: must not be '.' or '..'", e.getMessage());
        e = expectThrows(InvalidAliasNameException.class, () -> validator.validateAliasStandalone("..", null));
        assertEquals("Invalid alias name [..]: must not be '.' or '..'", e.getMessage());
        e = expectThrows(InvalidAliasNameException.class, () -> validator.validateAliasStandalone("_cat", null));
        assertEquals("Invalid alias name [_cat]: must not start with '_', '-', or '+'", e.getMessage());
        e = expectThrows(InvalidAliasNameException.class, () -> validator.validateAliasStandalone("-cat", null));
        assertEquals("Invalid alias name [-cat]: must not start with '_', '-', or '+'", e.getMessage());
        e = expectThrows(InvalidAliasNameException.class, () -> validator.validateAliasStandalone("+cat", null));
        assertEquals("Invalid alias name [+cat]: must not start with '_', '-', or '+'", e.getMessage());
        e = expectThrows(InvalidAliasNameException.class, () -> validator.validateAliasStandalone("c*t", null));
        assertThat(e.getMessage(), startsWith("Invalid alias name [c*t]: must not contain the following characters "));

        // Doesn't throw an exception because we allow upper case alias names
        validator.validateAliasStandalone("CAT", null);
    }
}
