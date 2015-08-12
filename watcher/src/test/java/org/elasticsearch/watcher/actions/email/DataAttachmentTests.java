/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.actions.email.service.Attachment;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.hamcrest.Matchers.is;

/**
 *
 */
public class DataAttachmentTests extends ESTestCase {

    @Test
    public void testCreate_Json() throws Exception {
        Map<String, Object> data = ImmutableMap.<String, Object>of("key", "value");
        Attachment attachment = DataAttachment.JSON.create(data);
        InputStream input = attachment.bodyPart().getDataHandler().getInputStream();
        String content = Streams.copyToString(new InputStreamReader(input, StandardCharsets.UTF_8));
        assertThat(content, is("{" + System.lineSeparator() + "  \"key\" : \"value\"" + System.lineSeparator() + "}"));
    }

    @Test
    public void testCreate_Yaml() throws Exception {
        Map<String, Object> data = ImmutableMap.<String, Object>of("key", "value");
        Attachment attachment = DataAttachment.YAML.create(data);
        InputStream input = attachment.bodyPart().getDataHandler().getInputStream();
        String content = Streams.copyToString(new InputStreamReader(input, StandardCharsets.UTF_8));
        // the yaml factory in es always emits unix line breaks
        // this seems to be a bug in jackson yaml factory that doesn't default to the platform line break
        assertThat(content, is("---\nkey: \"value\"\n"));
    }
}
