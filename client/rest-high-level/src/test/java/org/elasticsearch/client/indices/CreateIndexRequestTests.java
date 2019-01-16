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
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Map;
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
        CreateIndexRequest request = new CreateIndexRequest("index");
        request.source(parser.map(), LoggingDeprecationHandler.INSTANCE);
        return request;
    }

    @Override
    protected void assertEqualInstances(CreateIndexRequest expectedInstance, CreateIndexRequest newInstance) {
        assertEquals(expectedInstance.settings(), newInstance.settings());
        assertAliasesEqual(expectedInstance.aliases(), newInstance.aliases());
        assertMappingsEqual(expectedInstance.mappings(), newInstance.mappings());
    }

    private void assertMappingsEqual(Map<String, String> expected, Map<String, String> actual) {
        assertEquals(expected.keySet(), actual.keySet());

        for (Map.Entry<String, String> expectedEntry : expected.entrySet()) {
            String expectedValue = expectedEntry.getValue();
            String actualValue = actual.get(expectedEntry.getKey());
            try (XContentParser expectedJson = createParser(JsonXContent.jsonXContent, expectedValue);
                 XContentParser actualJson = createParser(JsonXContent.jsonXContent, actualValue)) {
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
