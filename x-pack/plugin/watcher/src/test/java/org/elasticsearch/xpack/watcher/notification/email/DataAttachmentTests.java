/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.test.ESTestCase;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.is;

public class DataAttachmentTests extends ESTestCase {

    public void testCreateJson() throws Exception {
        Map<String, Object> data = singletonMap("key", "value");
        Attachment attachment = DataAttachment.JSON.create("data", data);
        InputStream input = attachment.bodyPart().getDataHandler().getInputStream();
        String content = Streams.copyToString(new InputStreamReader(input, StandardCharsets.UTF_8));
        assertThat(content, is("{\n  \"key\" : \"value\"\n}"));
    }

    public void testCreateYaml() throws Exception {
        Map<String, Object> data = singletonMap("key", "value");
        Attachment attachment = DataAttachment.YAML.create("data", data);
        InputStream input = attachment.bodyPart().getDataHandler().getInputStream();
        String content = Streams.copyToString(new InputStreamReader(input, StandardCharsets.UTF_8));
        // the yaml factory in es always emits unix line breaks
        // this seems to be a bug in jackson yaml factory that doesn't default to the platform line break
        assertThat(content, is("---\nkey: \"value\"\n"));
    }
}
