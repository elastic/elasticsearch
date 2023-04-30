/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;

public class AttachmentProcessorFactoryTests extends ESTestCase {

    private AttachmentProcessor.Factory factory = new AttachmentProcessor.Factory();

    public void testBuildDefaults() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");

        String processorTag = randomAlphaOfLength(10);

        AttachmentProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("attachment"));
        assertThat(processor.getProperties(), sameInstance(AttachmentProcessor.Factory.DEFAULT_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());

        assertWarnings(
            "The default [remove_binary] value of 'false' is deprecated "
                + "and will be set to 'true' in a future release. Set [remove_binary] explicitly to 'true'"
                + " or 'false' to ensure no behavior change."
        );
    }

    public void testConfigureIndexedChars() throws Exception {
        int indexedChars = randomIntBetween(1, 100000);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("indexed_chars", indexedChars);

        String processorTag = randomAlphaOfLength(10);
        AttachmentProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getIndexedChars(), is(indexedChars));
        assertFalse(processor.isIgnoreMissing());

        assertWarnings(
            "The default [remove_binary] value of 'false' is deprecated "
                + "and will be set to 'true' in a future release. Set [remove_binary] explicitly to 'true'"
                + " or 'false' to ensure no behavior change."
        );
    }

    public void testBuildTargetField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("target_field", "_field");
        AttachmentProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("_field"));
        assertFalse(processor.isIgnoreMissing());

        assertWarnings(
            "The default [remove_binary] value of 'false' is deprecated "
                + "and will be set to 'true' in a future release. Set [remove_binary] explicitly to 'true'"
                + " or 'false' to ensure no behavior change."
        );
    }

    public void testBuildFields() throws Exception {
        Set<AttachmentProcessor.Property> properties = EnumSet.noneOf(AttachmentProcessor.Property.class);
        List<String> fieldNames = new ArrayList<>();
        int numFields = scaledRandomIntBetween(1, AttachmentProcessor.Property.values().length);
        for (int i = 0; i < numFields; i++) {
            AttachmentProcessor.Property property = AttachmentProcessor.Property.values()[i];
            properties.add(property);
            fieldNames.add(property.name().toLowerCase(Locale.ROOT));
        }
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", fieldNames);
        AttachmentProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), equalTo(properties));
        assertFalse(processor.isIgnoreMissing());

        assertWarnings(
            "The default [remove_binary] value of 'false' is deprecated "
                + "and will be set to 'true' in a future release. Set [remove_binary] explicitly to 'true'"
                + " or 'false' to ensure no behavior change."
        );
    }

    public void testBuildIllegalFieldOption() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", Collections.singletonList("invalid"));
        try {
            factory.create(null, null, null, config);
            fail("exception expected");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("[properties] illegal field option [invalid]"));
            // ensure allowed fields are mentioned
            for (AttachmentProcessor.Property property : AttachmentProcessor.Property.values()) {
                assertThat(e.getMessage(), containsString(property.name()));
            }
        }

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", "invalid");
        try {
            factory.create(null, null, null, config);
            fail("exception expected");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[properties] property isn't a list, but of type [java.lang.String]"));
        }

        assertWarnings(
            "The default [remove_binary] value of 'false' is deprecated "
                + "and will be set to 'true' in a future release. Set [remove_binary] explicitly to 'true'"
                + " or 'false' to ensure no behavior change."
        );
    }

    public void testIgnoreMissing() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("ignore_missing", true);

        String processorTag = randomAlphaOfLength(10);

        AttachmentProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("attachment"));
        assertThat(processor.getProperties(), sameInstance(AttachmentProcessor.Factory.DEFAULT_PROPERTIES));
        assertTrue(processor.isIgnoreMissing());

        assertWarnings(
            "The default [remove_binary] value of 'false' is deprecated "
                + "and will be set to 'true' in a future release. Set [remove_binary] explicitly to 'true'"
                + " or 'false' to ensure no behavior change."
        );
    }

    public void testRemoveBinary() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("remove_binary", true);

        String processorTag = randomAlphaOfLength(10);

        AttachmentProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("attachment"));
        assertThat(processor.getProperties(), sameInstance(AttachmentProcessor.Factory.DEFAULT_PROPERTIES));
        assertTrue(processor.isRemoveBinary());
    }
}
