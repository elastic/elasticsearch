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
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class ApplicationResourcePrivilegesTests extends AbstractXContentTestCase<ApplicationResourcePrivileges> {

    public static ApplicationResourcePrivileges createNewRandom(String name) {
        return new ApplicationResourcePrivileges(name,
                Arrays.asList(randomArray(1, 8, size -> new String[size], () -> randomAlphaOfLengthBetween(1, 8))),
                Arrays.asList(randomArray(1, 8, size -> new String[size], () -> randomAlphaOfLengthBetween(1, 8))));
    }

    @Override
    protected ApplicationResourcePrivileges createTestInstance() {
        return createNewRandom(randomAlphaOfLengthBetween(1, 8));
    }

    @Override
    protected ApplicationResourcePrivileges doParseInstance(XContentParser parser) throws IOException {
        return ApplicationResourcePrivileges.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testEmptyApplicationName() {
        final String emptyApplicationName = randomBoolean() ? "" : null;
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new ApplicationResourcePrivileges(emptyApplicationName,
                        Arrays.asList(randomArray(1, 8, size -> new String[size], () -> randomAlphaOfLengthBetween(1, 8))),
                        Arrays.asList(randomArray(1, 8, size -> new String[size], () -> randomAlphaOfLengthBetween(1, 8)))));
        assertThat(e.getMessage(), is("application privileges must have an application name"));
    }

    public void testEmptyPrivileges() {
        final List<String> emptyPrivileges = randomBoolean() ? Collections.emptyList() : null;
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new ApplicationResourcePrivileges(randomAlphaOfLengthBetween(1, 8),
                        emptyPrivileges,
                        Arrays.asList(randomArray(1, 8, size -> new String[size], () -> randomAlphaOfLengthBetween(1, 8)))));
        assertThat(e.getMessage(), is("application privileges must define at least one privilege"));
    }

    public void testEmptyResources() {
        final List<String> emptyResources = randomBoolean() ? Collections.emptyList() : null;
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new ApplicationResourcePrivileges(randomAlphaOfLengthBetween(1, 8),
                        Arrays.asList(randomArray(1, 8, size -> new String[size], () -> randomAlphaOfLengthBetween(1, 8))),
                        emptyResources));
        assertThat(e.getMessage(), is("application privileges must refer to at least one resource"));
    }
}
