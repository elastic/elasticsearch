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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.tasks.TaskId;
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

    public void testCanCoalesceWith() {
        MigrateIndexRequest request = randomRequest();
        assertTrue("reflexivity of " + request, request.canCoalesceWith(request));
        MigrateIndexRequest copy = copy(request);
        assertTrue("basic of " + request + " with " + copy, request.canCoalesceWith(copy));
        assertTrue("symmetric of " + request + " with " + copy, copy.canCoalesceWith(request));
        assertTrue("consistent of " + request + " with " + copy, request.canCoalesceWith(copy));

        mutateNonCoalesceWithField(copy);
        assertTrue("basic of " + request + " with " + copy, request.canCoalesceWith(copy));
        assertTrue("symmetric of " + request + " with " + copy, copy.canCoalesceWith(request));
        assertTrue("consistent of " + request + " with " + copy, request.canCoalesceWith(copy));

        MigrateIndexRequest mutant = copy(request);
        mutateCoalesceWithField(mutant);
        assertFalse("basic of " + request + " with " + mutant, request.canCoalesceWith(mutant));
        assertFalse("symmetric of " + mutant + " with " + request, mutant.canCoalesceWith(request));
        assertFalse("consistent of " + request + " with " + mutant, request.canCoalesceWith(mutant));
    }

    public void testEquals() {
        MigrateIndexRequest request = randomRequest();
        assertEquals("reflexivity", request, request);
        MigrateIndexRequest copy = copy(request);
        assertEquals("basic", request, copy);
        assertEquals("symmetric", copy, request);
        assertEquals("consistent", request, copy);

        MigrateIndexRequest mutant = copy(request);
        if (randomBoolean()) {
            mutateCoalesceWithField(mutant);
            if (randomBoolean()) {
                mutateNonCoalesceWithField(mutant);
            }
        } else {
            mutateNonCoalesceWithField(mutant);
        }
        assertNotEquals("basic of " + request + " with " + mutant, request, mutant);
        assertNotEquals("symmetric of " + mutant + " with " + request, mutant, request);
        assertNotEquals("consistent of " + request + " with " + mutant, request, mutant);
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

    private MigrateIndexRequest copy(MigrateIndexRequest original) {
        MigrateIndexRequest copy = new MigrateIndexRequest(original.getSourceIndex(), original.getCreateIndexRequest().index());
        // Settings are immutable so it is safe to use a reference
        copy.getCreateIndexRequest().settings(original.getCreateIndexRequest().settings());
        for (Map.Entry<String, String> type : original.getCreateIndexRequest().mappings().entrySet()) {
            copy.getCreateIndexRequest().mapping(type.getKey(), type.getValue());
        }
        for (Alias alias : original.getCreateIndexRequest().aliases()) {
            Alias aliasCopy = new Alias(alias.name()).filter(alias.filter());
            aliasCopy.indexRouting(alias.indexRouting()).searchRouting(alias.searchRouting());
            copy.getCreateIndexRequest().alias(aliasCopy);
        }
        copy.setScript(original.getScript()); // Script is immutable so it is safe to use a reference
        copy.timeout(original.timeout());
        copy.masterNodeTimeout(original.masterNodeTimeout());
        copy.setParentTask(original.getParentTask());
        return copy;
    }

    /**
     * Mutate one field that {@link MigrateIndexRequest#canCoalesceWith(MigrateIndexRequest)} does care about.
     */
    private void mutateCoalesceWithField(MigrateIndexRequest request) {
        switch (between(0, 3)) {
        case 0: // Mutate settings
            Settings.Builder newSettings = Settings.builder().put(request.getCreateIndexRequest().settings());
            if (request.getCreateIndexRequest().settings().isEmpty()) {
                newSettings.put(randomAsciiOfLength(5), randomAsciiOfLength(5));
            } else {
                switch (between(0, 2)) {
                case 0:
                    // Remove
                    assertNotNull(newSettings.remove(randomFrom(request.getCreateIndexRequest().settings().getAsMap().keySet())));
                    break;
                case 1:
                    // Add
                    String key = randomValueOtherThanMany(k -> request.getCreateIndexRequest().settings().getAsMap().containsKey(k),
                            () -> randomAsciiOfLength(5));
                    newSettings.put(key, randomAsciiOfLength(5));
                    break;
                case 2:
                    // Mutate value
                    key = randomFrom(request.getCreateIndexRequest().settings().getAsMap().keySet());
                    assertNotNull(newSettings.put(key, request.getCreateIndexRequest().settings().get(key) + "mutated"));
                    break;
                default:
                    throw new UnsupportedOperationException();
                }
            }
            request.getCreateIndexRequest().settings(newSettings);
            break;
        case 1: // Mutate mapping
            if (request.getCreateIndexRequest().mappings().isEmpty()) {
                request.getCreateIndexRequest().mapping(randomAsciiOfLength(5), randomAsciiOfLength(5));
            } else {
                switch (between(0, 2)) {
                case 0:
                    // Remove
                    assertNotNull(request.getCreateIndexRequest().mappings()
                            .remove(randomFrom(request.getCreateIndexRequest().mappings().keySet())));
                    break;
                case 1:
                    // Add
                    String key = randomValueOtherThanMany(k -> request.getCreateIndexRequest().mappings().containsKey(k),
                            () -> randomAsciiOfLength(5));
                    request.getCreateIndexRequest().mapping(key, randomAsciiOfLength(5));
                    break;
                case 2:
                    // Mutate value
                    key = randomFrom(request.getCreateIndexRequest().mappings().keySet());
                    assertNotNull(request.getCreateIndexRequest().mappings().remove(key));
                    request.getCreateIndexRequest().mapping(key, request.getCreateIndexRequest().mappings().get(key) + "mutated");
                    break;
                default:
                    throw new UnsupportedOperationException();
                }
            }
            break;
        case 2: // Mutate alias
            if (request.getCreateIndexRequest().aliases().isEmpty()) {
                request.getCreateIndexRequest().alias(randomAlias());
            } else {
                switch (between(0, 1)) {
                case 0:
                    // Remove
                    assertTrue(request.getCreateIndexRequest().aliases().remove(randomFrom(request.getCreateIndexRequest().aliases())));
                    break;
                case 1:
                    // Add
                    Alias alias = randomValueOtherThanMany(
                            a -> request.getCreateIndexRequest().aliases().stream().anyMatch(old -> a.name().equals(old.name())),
                            () -> randomAlias());
                    request.getCreateIndexRequest().alias(alias);
                    break;
                default:
                    throw new UnsupportedOperationException();
                }
            }
            break;
        case 3:
            // Mutate script
            if (request.getScript() == null) {
                request.setScript(randomScript());
            } else {
                if (randomBoolean()) {
                    request.setScript(new Script(request.getScript().getScript() + "mutated", request.getScript().getType(),
                            request.getScript().getLang(), request.getScript().getParams()));
                } else {
                    request.setScript(null);
                }
            }
            break;
        default:
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Mutate one field that {@link MigrateIndexRequest#canCoalesceWith(MigrateIndexRequest)} doesn't care about.
     */
    private void mutateNonCoalesceWithField(MigrateIndexRequest request) {
        switch (between(0, 2)) {
        case 0: // Mutate timeout
            request.timeout(randomValueOtherThan(request.timeout(), () -> TimeValue.parseTimeValue(randomPositiveTimeValue(), "test")));
            break;
        case 1: // Mutate master timeout
            request.masterNodeTimeout(
                    randomValueOtherThan(request.masterNodeTimeout(), () -> TimeValue.parseTimeValue(randomPositiveTimeValue(), "test")));
            break;
        case 2: // Mutate parent task id
            if (request.getParentTask().equals(TaskId.EMPTY_TASK_ID)) {
                request.setParentTask(randomAsciiOfLength(5), randomLong());
            } else {
                if (randomBoolean()) {
                    request.setParentTask(TaskId.EMPTY_TASK_ID);
                } else {
                    request.setParentTask(
                            randomValueOtherThan(request.getParentTask(), () -> new TaskId(randomAsciiOfLength(5), randomLong())));
                }
            }
            break;
        default:
            throw new UnsupportedOperationException();
        }            
    }
}
