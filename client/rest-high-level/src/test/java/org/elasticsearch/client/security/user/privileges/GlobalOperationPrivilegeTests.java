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

package org.elasticsearch.client.security.user.privileges;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class GlobalOperationPrivilegeTests extends ESTestCase {

    public void testConstructor() {
        final String category = randomFrom(Arrays.asList(null, randomAlphaOfLength(5)));
        final String operation = randomFrom(Arrays.asList(null, randomAlphaOfLength(5)));
        final Map<String, Object> privilege = randomFrom(Arrays.asList(null, Collections.emptyMap(), Collections.singletonMap("k1", "v1")));

        if (Strings.hasText(category) && Strings.hasText(operation) && privilege != null && privilege.isEmpty() == false) {
            GlobalOperationPrivilege globalOperationPrivilege = new GlobalOperationPrivilege(category, operation, privilege);
            assertThat(globalOperationPrivilege.getCategory(), equalTo(category));
            assertThat(globalOperationPrivilege.getOperation(), equalTo(operation));
            assertThat(globalOperationPrivilege.getRaw(), equalTo(privilege));
        } else {
            if (category == null || operation == null) {
                expectThrows(NullPointerException.class,
                        () -> new GlobalOperationPrivilege(category, operation, privilege));
            } else {
                final IllegalArgumentException ile = expectThrows(IllegalArgumentException.class,
                        () -> new GlobalOperationPrivilege(category, operation, privilege));
                assertThat(ile.getMessage(), equalTo("privileges cannot be empty or null"));
            }
        }
    }

    public void testEqualsHashCode() {
        final String category = randomAlphaOfLength(5);
        final String operation = randomAlphaOfLength(5);
        final Map<String, Object> privilege = Collections.singletonMap(randomAlphaOfLength(4), randomAlphaOfLength(5));
        GlobalOperationPrivilege globalOperationPrivilege = new GlobalOperationPrivilege(category, operation, privilege);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(globalOperationPrivilege, (original) -> {
            return new GlobalOperationPrivilege(original.getCategory(), original.getOperation(), original.getRaw());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(globalOperationPrivilege, (original) -> {
            return new GlobalOperationPrivilege(original.getCategory(), original.getOperation(), original.getRaw());
        }, GlobalOperationPrivilegeTests::mutateTestItem);
    }

    private static GlobalOperationPrivilege mutateTestItem(GlobalOperationPrivilege original) {
        switch (randomIntBetween(0, 2)) {
        case 0:
            return new GlobalOperationPrivilege(randomAlphaOfLength(5), original.getOperation(), original.getRaw());
        case 1:
            return new GlobalOperationPrivilege(original.getCategory(), randomAlphaOfLength(5), original.getRaw());
        case 2:
            return new GlobalOperationPrivilege(original.getCategory(), original.getOperation(),
                    Collections.singletonMap(randomAlphaOfLength(4), randomAlphaOfLength(4)));
        default:
            return new GlobalOperationPrivilege(randomAlphaOfLength(5), original.getOperation(), original.getRaw());
        }
    }
}
