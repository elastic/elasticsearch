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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlobalPrivilegesTests extends AbstractXContentTestCase<GlobalPrivileges> {

    @Override
    protected GlobalPrivileges createTestInstance() {
        final List<GlobalScopedPrivilege> privilegeList = Arrays
                .asList(randomArray(0, 2, size -> new GlobalScopedPrivilege[size], () -> buildRandomGlobalScopedPrivilege()));
        return new GlobalPrivileges(privilegeList);
    }

    @Override
    protected GlobalPrivileges doParseInstance(XContentParser parser) throws IOException {
        return GlobalPrivileges.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false; // true really means inserting bogus privileges
    }

    private static GlobalScopedPrivilege buildRandomGlobalScopedPrivilege() {
        final Map<String, Object> privilege = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 3); i++) {
            privilege.put(randomAlphaOfLength(8), "");
        }
        return new GlobalScopedPrivilege(randomAlphaOfLengthBetween(0, 2), privilege);
    }
}
