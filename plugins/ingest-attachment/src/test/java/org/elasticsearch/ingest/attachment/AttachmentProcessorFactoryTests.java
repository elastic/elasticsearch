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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.attachment.AttachmentProcessor.CONTENT;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.CONTENT_LENGTH;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.CONTENT_TYPE;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.RESERVED_PROPERTIES;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.RESERVED_PROPERTIES_KEYS;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.asReservedProperty;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
        assertThat(processor.getReservedProperties(), sameInstance(RESERVED_PROPERTIES_KEYS));
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
        List<String> properties = new ArrayList<>();
        int numFields = scaledRandomIntBetween(1, RESERVED_PROPERTIES_KEYS.size());
        Iterator<String> iterator = RESERVED_PROPERTIES_KEYS.iterator();
        for (int i = 0; i < numFields; i++) {
            String reservedProperty = iterator.next();
            properties.add(reservedProperty);
        }
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", properties);
        AttachmentProcessor processor = factory.create(null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), containsInAnyOrder(properties.toArray()));
        assertThat(processor.getReservedProperties(), containsInAnyOrder(properties.toArray()));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildDeprecatedReservedProperties() throws Exception {
        Set<String> reservedProperties = new HashSet<>();
        List<String> properties = new ArrayList<>();
        List<String> expectedWarnings = new ArrayList<>();
        int numFields = scaledRandomIntBetween(1, RESERVED_PROPERTIES.size());
        Iterator<String> iterator = RESERVED_PROPERTIES.iterator();
        for (int i = 0; i < numFields; i++) {
            String property = iterator.next();
            properties.add(property);
            String reservedProperty = asReservedProperty(property);
            reservedProperties.add(reservedProperty);
            expectedWarnings.add("[" + property + "] should be replaced with [" + reservedProperty + "]");
        }
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", properties);
        AttachmentProcessor processor = factory.create(null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), containsInAnyOrder(properties.toArray()));
        assertThat(processor.getReservedProperties(), equalTo(reservedProperties));
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
        assertThat(processor.getReservedProperties(), sameInstance(RESERVED_PROPERTIES_KEYS));
        assertTrue(processor.isIgnoreMissing());
    }

    public void testBuildWildcardAllProperties() throws Exception {
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("*");
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", fieldNames);
        AttachmentProcessor processor = factory.create(null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), contains("*"));
        assertThat(processor.getReservedProperties(), equalTo(RESERVED_PROPERTIES_KEYS));
    }


    public void testBuildWildcardAllReservedProperties() throws Exception {
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("_*_");
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", fieldNames);
        AttachmentProcessor processor = factory.create(null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), contains("_*_"));
        assertThat(processor.getReservedProperties(), equalTo(RESERVED_PROPERTIES_KEYS));
    }

    public void testBuildWildcardSomeReservedProperties() throws Exception {
        Set<String> properties = Sets.newHashSet(
            asReservedProperty(CONTENT),
            asReservedProperty(CONTENT_LENGTH),
            asReservedProperty(CONTENT_TYPE));
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("_content*");
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", fieldNames);
        AttachmentProcessor processor = factory.create(null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), contains("_content*"));
        assertThat(processor.getReservedProperties(), equalTo(properties));
    }
}
