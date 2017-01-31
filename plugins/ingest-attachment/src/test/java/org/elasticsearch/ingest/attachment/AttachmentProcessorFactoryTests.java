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

package org.elasticsearch.ingest.attachment;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;

public class AttachmentProcessorFactoryTests extends ESTestCase {

    private AttachmentProcessor.Factory factory = new AttachmentProcessor.Factory();

    public void testBuildDefaults() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");

        String processorTag = randomAsciiOfLength(10);

        AttachmentProcessor processor = factory.create(null, processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("attachment"));
        assertThat(processor.getProperties(), emptyIterable());
        assertThat(processor.getReservedProperties(), sameInstance(AttachmentProcessor.Factory.DEFAULT_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testConfigureIndexedChars() throws Exception {
        int indexedChars = randomIntBetween(1, 100000);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("indexed_chars", indexedChars);

        String processorTag = randomAsciiOfLength(10);
        AttachmentProcessor processor = factory.create(null, processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getIndexedChars(), is(indexedChars));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildTargetField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("target_field", "_field");
        AttachmentProcessor processor = factory.create(null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("_field"));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildReservedProperties() throws Exception {
        Set<AttachmentProcessor.ReservedProperty> properties = EnumSet.noneOf(AttachmentProcessor.ReservedProperty.class);
        List<String> fieldNames = new ArrayList<>();
        int numFields = scaledRandomIntBetween(1, AttachmentProcessor.ReservedProperty.values().length);
        for (int i = 0; i < numFields; i++) {
            AttachmentProcessor.ReservedProperty reservedProperty = AttachmentProcessor.ReservedProperty.values()[i];
            properties.add(reservedProperty);
            fieldNames.add(reservedProperty.key);
        }
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", fieldNames);
        AttachmentProcessor processor = factory.create(null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), emptyIterable());
        assertThat(processor.getReservedProperties(), equalTo(properties));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildDeprecatedReservedProperties() throws Exception {
        Set<AttachmentProcessor.ReservedProperty> properties = EnumSet.noneOf(AttachmentProcessor.ReservedProperty.class);
        List<String> fieldNames = new ArrayList<>();
        List<String> expectedWarnings = new ArrayList<>();
        int numFields = scaledRandomIntBetween(1, AttachmentProcessor.ReservedProperty.values().length);
        for (int i = 0; i < numFields; i++) {
            AttachmentProcessor.ReservedProperty reservedProperty = AttachmentProcessor.ReservedProperty.values()[i];
            properties.add(reservedProperty);
            String deprecatedName = reservedProperty.name().toLowerCase(Locale.ROOT);
            fieldNames.add(deprecatedName);
            expectedWarnings.add("[" + deprecatedName + "] should be replaced with [" + reservedProperty.key + "]");
        }
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", fieldNames);
        AttachmentProcessor processor = factory.create(null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), emptyIterable());
        assertThat(processor.getReservedProperties(), equalTo(properties));
        assertWarnings(expectedWarnings.toArray(new String[]{}));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildIllegalFieldOption() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", "invalid");
        try {
            factory.create(null, null, config);
            fail("exception expected");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[properties] property isn't a list, but of type [java.lang.String]"));
        }
    }

    public void testIgnoreMissing() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("ignore_missing", true);

        String processorTag = randomAsciiOfLength(10);

        AttachmentProcessor processor = factory.create(null, processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("attachment"));
        assertThat(processor.getProperties(), emptyIterable());
        assertThat(processor.getReservedProperties(), sameInstance(AttachmentProcessor.Factory.DEFAULT_PROPERTIES));
        assertTrue(processor.isIgnoreMissing());
    }
}
