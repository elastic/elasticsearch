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

package org.elasticsearch.ingest;

import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class IngestModuleTests extends ESTestCase {

    public void testIsIngestEnabledSettings() {
        assertThat(IngestModule.isIngestEnabled(Settings.EMPTY), equalTo(true));
        assertThat(IngestModule.isIngestEnabled(Settings.builder().put("node.ingest", true).build()), equalTo(true));
        assertThat(IngestModule.isIngestEnabled(Settings.builder().put("node.ingest", "true").build()), equalTo(true));
        assertThat(IngestModule.isIngestEnabled(Settings.builder().put("node.ingest", false).build()), equalTo(false));

        assertThat(IngestModule.isIngestEnabled(Settings.builder().put("node.ingest", "false").build()), equalTo(false));
        assertThat(IngestModule.isIngestEnabled(Settings.builder().put("node.ingest", "off").build()), equalTo(false));
        assertThat(IngestModule.isIngestEnabled(Settings.builder().put("node.ingest", "no").build()), equalTo(false));
        assertThat(IngestModule.isIngestEnabled(Settings.builder().put("node.ingest", "0").build()), equalTo(false));
    }

    public void testIsIngestEnabledAttributes() {
        assertThat(IngestModule.isIngestEnabled(ImmutableOpenMap.<String, String>builder().build()), equalTo(true));

        ImmutableOpenMap.Builder<String, String> builder = ImmutableOpenMap.<String, String>builder();
        builder.put("ingest", "true");
        assertThat(IngestModule.isIngestEnabled(builder.build()), equalTo(true));

        builder = ImmutableOpenMap.<String, String>builder();
        builder.put("ingest", "false");
        assertThat(IngestModule.isIngestEnabled(builder.build()), equalTo(false));

        builder = ImmutableOpenMap.<String, String>builder();
        builder.put("ingest", "off");
        assertThat(IngestModule.isIngestEnabled(builder.build()), equalTo(false));

        builder = ImmutableOpenMap.<String, String>builder();
        builder.put("ingest", "no");
        assertThat(IngestModule.isIngestEnabled(builder.build()), equalTo(false));

        builder = ImmutableOpenMap.<String, String>builder();
        builder.put("ingest", "0");
        assertThat(IngestModule.isIngestEnabled(builder.build()), equalTo(false));
    }

    public void testIsIngestEnabledMethodsReturnTheSameValue() {
        String randomString;
        if (randomBoolean()) {
            randomString = randomFrom("true", "false", "on", "off", "yes", "no", "0", "1");
        } else {
            randomString = randomAsciiOfLengthBetween(1, 5);
        }
        Settings settings = Settings.builder().put("node.ingest", randomString).build();
        ImmutableOpenMap.Builder<String, String> builder = ImmutableOpenMap.<String, String>builder();
        builder.put("ingest", randomString);
        ImmutableOpenMap<String, String> attributes = builder.build();

        assertThat(IngestModule.isIngestEnabled(settings), equalTo(IngestModule.isIngestEnabled(attributes)));
    }
}
