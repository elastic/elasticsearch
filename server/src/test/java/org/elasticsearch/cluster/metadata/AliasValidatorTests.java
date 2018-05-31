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

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
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

    public void testValidateAliasWriteOnly() {
        AliasValidator validator = new AliasValidator(Settings.EMPTY);
        String alias = randomAlphaOfLength(5);
        String index = randomAlphaOfLength(10);

        // when false
        validator.validateAliasWriteOnly(alias, false, buildMetaData(index, alias, randomBoolean()));

        // when true and conflicts with existing index
        Exception exception = expectThrows(IllegalArgumentException.class,
            () -> validator.validateAliasWriteOnly(alias, true, buildMetaData(index, alias, true)));
        assertThat(exception.getMessage(), equalTo("alias [" + alias + "] already has a write index [" + index + "]"));

        // when true and safe
        validator.validateAliasWriteOnly(alias, true, buildMetaData(index, alias, false));
    }

    private MetaData buildMetaData(String name, String alias, boolean writeIndex) {
        return MetaData.builder().put(IndexMetaData.builder(name)
            .settings(settings(Version.CURRENT)).creationDate(randomNonNegativeLong())
            .putAlias(AliasMetaData.builder(alias).writeIndex(writeIndex))
            .numberOfShards(1).numberOfReplicas(0)).build();
    }
}
