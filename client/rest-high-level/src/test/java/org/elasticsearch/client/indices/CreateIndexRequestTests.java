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

package org.elasticsearch.client.indices;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.client.indices.CreateIndexRequest.ALIASES;
import static org.elasticsearch.client.indices.CreateIndexRequest.MAPPINGS;
import static org.elasticsearch.client.indices.CreateIndexRequest.SETTINGS;

public class CreateIndexRequestTests extends AbstractXContentTestCase<CreateIndexRequest> {

    @Override
    protected CreateIndexRequest createTestInstance() {
        return RandomCreateIndexGenerator.randomCreateIndexRequest();
    }

    @Override
    protected CreateIndexRequest doParseInstance(XContentParser parser) throws IOException {
        return new CreateIndexRequest("index").source(parser.map());
    }

    @Override
    protected void assertEqualInstances(CreateIndexRequest expected, CreateIndexRequest actual) {
        assertEquals(expected.settings(), actual.settings());
        assertAliasesEqual(expected.aliases(), actual.aliases());
        assertMappingsEqual(expected, actual);
    }

    private void assertMappingsEqual(CreateIndexRequest expected, CreateIndexRequest actual) {
        if (expected.mappings() == null) {
            assertNull(actual.mappings());
        } else {
            assertNotNull(actual.mappings());
            try (XContentParser expectedJson = createParser(expected.mappingsXContentType().xContent(), expected.mappings());
                 XContentParser actualJson = createParser(actual.mappingsXContentType().xContent(), actual.mappings())) {
                assertEquals(expectedJson.map(), actualJson.map());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void assertAliasesEqual(Set<Alias> expected, Set<Alias> actual) {
        assertEquals(expected, actual);

        for (Alias expectedAlias : expected) {
            for (Alias actualAlias : actual) {
                if (expectedAlias.equals(actualAlias)) {
                    // As Alias#equals only looks at name, we check the equality of the other Alias parameters here.
                    assertEquals(expectedAlias.filter(), actualAlias.filter());
                    assertEquals(expectedAlias.indexRouting(), actualAlias.indexRouting());
                    assertEquals(expectedAlias.searchRouting(), actualAlias.searchRouting());
                }
            }
        }
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.startsWith(MAPPINGS.getPreferredName())
            || field.startsWith(SETTINGS.getPreferredName())
            || field.startsWith(ALIASES.getPreferredName());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
