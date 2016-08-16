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

package org.elasticsearch.action.admin.indices.migrate;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class MigrateIndexRequestTests extends ESTestCase {
    public void testRoundTripThroughTransport() throws IOException {
        MigrateIndexRequest original = randomRequest();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                MigrateIndexRequest read = new MigrateIndexRequest();
                read.readFrom(in);
                assertEquals(original, read);
            }
        }
    }

    public void testToStringIsSane() {
        String string = randomRequest().toString();
        assertThat(string, containsString("MigrateIndex["));
        assertThat(string, containsString("source="));
        assertThat(string, containsString("create="));
        assertThat(string, containsString("script="));
        assertThat(string, containsString("timeout="));
        assertThat(string, containsString("masterNodeTimeout="));
        assertThat(string, containsString("parentTask="));
    }

    public void testValidation() {
        MigrateIndexRequest request = new MigrateIndexRequest("test_0", "test_1");
        request.getCreateIndexRequest().alias(new Alias("test"));
        assertNull(request.validate());

        request = new MigrateIndexRequest();
        request.setCreateIndexRequest(new CreateIndexRequest("test_1").alias(new Alias("test")));
        ValidationException e = request.validate();
        assertEquals("Validation Failed: 1: source index is not set;", e.getMessage());

        request = new MigrateIndexRequest();
        request.setSourceIndex("test_0");
        e = request.validate();
        assertEquals("Validation Failed: 1: create index request is not set;", e.getMessage());

        request = new MigrateIndexRequest();
        request.setSourceIndex("test_0");
        request.setCreateIndexRequest(new CreateIndexRequest().alias(new Alias("test")));
        e = request.validate();
        assertEquals("Validation Failed: 1: validation error with create index: index is missing;", e.getMessage());

        request = new MigrateIndexRequest("test_0", "test_0");
        request.getCreateIndexRequest().alias(new Alias("test"));
        e = request.validate();
        assertEquals("Validation Failed: 1: source and destination can't be the same index;", e.getMessage());

        request = new MigrateIndexRequest("test_0", "test_1");
        e = request.validate();
        assertEquals("Validation Failed: 1: migrating an index requires an alias;", e.getMessage());
        
        request = new MigrateIndexRequest("test_0", "test_1");
        request.getCreateIndexRequest().alias(new Alias("test_1"));
        e = request.validate();
        assertEquals("Validation Failed: 1: can't add an alias with the same name as the destination index [test_1];", e.getMessage());

        request = new MigrateIndexRequest("test_0", "test_1");
        request.getCreateIndexRequest().alias(new Alias("test"))
                .waitForActiveShards(randomFrom(ActiveShardCount.from(0), ActiveShardCount.NONE));
        e = request.validate();
        assertEquals("Validation Failed: 1: must wait for more than one active shard in the new index;", e.getMessage());
    }

    private MigrateIndexRequest randomRequest() {
        MigrateIndexRequest request = new MigrateIndexRequest(randomAsciiOfLength(5), randomAsciiOfLength(5));
        int settingsCount = between(0, 5);
        if (settingsCount > 0) {
            Settings.Builder settings = Settings.builder();
            for (int i = 0; i < settingsCount; i++) {
                settings.put(randomAsciiOfLength(5), randomAsciiOfLength(5));
            }
            request.getCreateIndexRequest().settings(settings);
        }
        int typesCount = between(0, 5);
        if (typesCount > 0) {
            for (int i = 0; i < typesCount; i++) {
                Map<String, Object> mapping = new HashMap<>();
                int mappingSize = between(0, 5);
                for (int p = 0; p < mappingSize; p++) {
                    mapping.put(randomAsciiOfLength(5), randomAsciiOfLength(5));
                }
                request.getCreateIndexRequest().mapping(randomAsciiOfLength(5), mapping);
            }
        }
        int aliasCount = between(0, 5);
        for (int i = 0; i < aliasCount; i++) {
            request.getCreateIndexRequest().alias(randomAlias());
        }
        if (randomBoolean()) {
            request.setScript(randomScript());
        }
        if (randomBoolean()) {
            request.timeout(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            request.masterNodeTimeout(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            request.setParentTask(randomAsciiOfLength(5), randomLong());
        }
        return request;
    }

    private Alias randomAlias() {
        Alias alias = new Alias(randomAsciiOfLength(5));
        if (randomBoolean()) {
            // We don't need a valid query for this test which is nice because random queries are hard
            alias.filter(randomAsciiOfLength(10));
        }
        if (randomBoolean()) {
            alias.indexRouting(randomAsciiOfLength(5));
        }
        if (randomBoolean()) {
            alias.searchRouting(randomAsciiOfLength(5));
        }
        return alias;
    }

    private Script randomScript() {
        int paramsLength = between(0, 10);
        Map<String, Object> params = new HashMap<>(paramsLength);
        for (int i = 0; i < paramsLength; i++) {
            params.put(randomAsciiOfLength(5), randomAsciiOfLength(5));
        }
        return new Script(randomAsciiOfLength(5), randomFrom(ScriptType.values()), randomAsciiOfLength(5), params);
    }
}
