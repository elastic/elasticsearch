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

package org.elasticsearch.ingest.common;

import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

public class FingerprintProcessorFactoryTests extends ESTestCase {

    private FingerprintProcessor.Factory factory = new FingerprintProcessor.Factory();

    public void testBuildDefaults() {
        Map<String, Object> config = new HashMap<>();
        List<String> fieldList = new ArrayList<>();
        fieldList.add("_field");
        config.put("fields", fieldList);
        String processorTag = randomAlphaOfLength(10);
        FingerprintProcessor processor = factory.create(null, processorTag, config);
        assertEquals(processorTag, processor.getTag());
        assertEquals(FingerprintProcessor.TYPE, processor.getType());
        Set<String> expectFeilds = new HashSet<>();
        expectFeilds.add("_field");
        assertEquals(expectFeilds, processor.getFields());
        assertEquals("fingerprint", processor.getTargetField());
        assertEquals(FingerprintProcessor.Method.MD5, processor.getMethod());
        assertFalse(processor.isBase64Encode());
        assertFalse(processor.isConcatenateAllFields());
        assertFalse(processor.isIgnoreMissing());
    }

    public void testValidMethod() {

        Map<String, Object> config = new HashMap<>();
        List<String> fieldList = new ArrayList<>();
        fieldList.add("_field");
        config.put("fields", fieldList);
        config.put("method", "SHA256");
        String processorTag = randomAlphaOfLength(10);
        FingerprintProcessor processor = factory.create(null, processorTag, config);
        assertEquals(processorTag, processor.getTag());
        assertEquals(FingerprintProcessor.TYPE, processor.getType());
        Set<String> expectFeilds = new HashSet<>();
        expectFeilds.add("_field");
        assertEquals(expectFeilds, processor.getFields());
        assertEquals("fingerprint", processor.getTargetField());
        assertEquals(FingerprintProcessor.Method.SHA256, processor.getMethod());
        assertFalse(processor.isBase64Encode());
        assertFalse(processor.isConcatenateAllFields());
        assertFalse(processor.isIgnoreMissing());

    }

    public void testInvalidMethod() {

        Map<String, Object> config = new HashMap<>();
        List<String> fieldList = new ArrayList<>();
        fieldList.add("_field");
        config.put("fields", fieldList);
        config.put("method", "Invalid method");

        String processorTag = randomAlphaOfLength(10);
        Exception exception = expectThrows(ElasticsearchParseException.class,
                () -> factory.create(null, processorTag, config));
        assertThat(exception.getMessage(), equalTo(
                "[method] illegal value [Invalid method]. Valid values are [SHA1, SHA256, MD5, MURMUR3, UUID]"));

    }

    public void testUUID() {
        Map<String, Object> config = new HashMap<>();
        config.put("method", "UUID");
        String processorTag = randomAlphaOfLength(10);
        FingerprintProcessor processor = factory.create(null, processorTag, config);
        assertEquals(processorTag, processor.getTag());
        assertEquals(FingerprintProcessor.TYPE, processor.getType());
        assertEquals(Collections.emptySet(), processor.getFields());
        assertEquals("fingerprint", processor.getTargetField());
        assertEquals(FingerprintProcessor.Method.UUID, processor.getMethod());
        assertFalse(processor.isBase64Encode());
        assertFalse(processor.isConcatenateAllFields());
        assertFalse(processor.isIgnoreMissing());
    }

    public void testUUIDWithFields() {
        Map<String, Object> config = new HashMap<>();
        config.put("method", "UUID");
        List<String> fieldList = new ArrayList<>();
        fieldList.add("_field");
        config.put("fields", fieldList);

        String processorTag = randomAlphaOfLength(10);
        Exception exception = expectThrows(ElasticsearchParseException.class,
                () -> factory.create(null, processorTag, config));
        assertThat(exception.getMessage(),
                equalTo("[method] is [UUID], [fields] or [concatenate_all_fields] must not be set"));
    }

    public void testUUIDWithConcatenateAllFields() {
        Map<String, Object> config = new HashMap<>();
        config.put("method", "UUID");
        List<String> fieldList = new ArrayList<>();
        fieldList.add("_field");
        config.put("fields", fieldList);

        String processorTag = randomAlphaOfLength(10);
        Exception exception = expectThrows(ElasticsearchParseException.class,
                () -> factory.create(null, processorTag, config));
        assertThat(exception.getMessage(),
                equalTo("[method] is [UUID], [fields] or [concatenate_all_fields] must not be set"));
    }

    public void testMD5WithMultiFields() {
        Map<String, Object> config = new HashMap<>();
        List<String> fieldList = new ArrayList<>();
        fieldList.add("field1");
        fieldList.add("field2");
        fieldList.add("field3");
        fieldList.add("field4");
        config.put("fields", fieldList);
        config.put("method", "MD5");
        String processorTag = randomAlphaOfLength(10);
        FingerprintProcessor processor = factory.create(null, processorTag, config);
        assertEquals(processorTag, processor.getTag());
        assertEquals(FingerprintProcessor.TYPE, processor.getType());
        Set<String> expectFeilds = new HashSet<>();
        expectFeilds.add("field1");
        expectFeilds.add("field2");
        expectFeilds.add("field3");
        expectFeilds.add("field4");
        assertEquals(expectFeilds, processor.getFields());
        assertEquals("fingerprint", processor.getTargetField());
        assertEquals(FingerprintProcessor.Method.MD5, processor.getMethod());
        assertFalse(processor.isBase64Encode());
        assertFalse(processor.isConcatenateAllFields());
        assertFalse(processor.isIgnoreMissing());
    }

    public void testMD5WithConcatenate() {
        Map<String, Object> config = new HashMap<>();
        config.put("method", "MD5");
        config.put("concatenate_all_fields", true);
        String processorTag = randomAlphaOfLength(10);
        FingerprintProcessor processor = factory.create(null, processorTag, config);
        assertEquals(processorTag, processor.getTag());
        assertEquals(FingerprintProcessor.TYPE, processor.getType());
        assertEquals(Collections.emptySet(), processor.getFields());
        assertEquals("fingerprint", processor.getTargetField());
        assertEquals(FingerprintProcessor.Method.MD5, processor.getMethod());
        assertFalse(processor.isBase64Encode());
        assertTrue(processor.isConcatenateAllFields());
        assertFalse(processor.isIgnoreMissing());
    }

    public void testMD5WithoutFieldsAndConcatenate() {
        Map<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        Exception exception = expectThrows(ElasticsearchParseException.class,
                () -> factory.create(null, processorTag, config));
        assertThat(exception.getMessage(),
                equalTo("[method] is [MD5], one of [fields] and [concatenate_all_fields] must be set"));
    }

    public void testMD5WithFieldsAndConcatenate() {
        Map<String, Object> config = new HashMap<>();
        List<String> fieldList = new ArrayList<>();
        fieldList.add("_field");
        config.put("fields", fieldList);
        config.put("method", "MD5");
        config.put("concatenate_all_fields", true);
        String processorTag = randomAlphaOfLength(10);
        Exception exception = expectThrows(ElasticsearchParseException.class,
                () -> factory.create(null, processorTag, config));
        assertThat(exception.getMessage(), equalTo("[fields] can not be set when [concatenate_all_fields] is true"));
    }

    public void testMD5WithEmptyFields() {
        Map<String, Object> config = new HashMap<>();
        List<String> fieldList = new ArrayList<>();
        config.put("fields", fieldList);
        config.put("method", "MD5");
        String processorTag = randomAlphaOfLength(10);
        Exception exception = expectThrows(ElasticsearchParseException.class,
                () -> factory.create(null, processorTag, config));
        assertThat(exception.getMessage(), equalTo("[fields] can not be empty"));
    }

    public void testMD5Base64Encode() {

        Map<String, Object> config = new HashMap<>();
        List<String> fieldList = new ArrayList<>();
        fieldList.add("_field");
        config.put("fields", fieldList);
        config.put("method", "MD5");
        config.put("base64_encode", true);

        String processorTag = randomAlphaOfLength(10);
        FingerprintProcessor processor = factory.create(null, processorTag, config);
        assertEquals(processorTag, processor.getTag());
        assertEquals(FingerprintProcessor.TYPE, processor.getType());
        Set<String> expectFeilds = new HashSet<>();
        expectFeilds.add("_field");
        assertEquals(expectFeilds, processor.getFields());
        assertEquals("fingerprint", processor.getTargetField());
        assertEquals(FingerprintProcessor.Method.MD5, processor.getMethod());
        assertTrue(processor.isBase64Encode());
        assertFalse(processor.isConcatenateAllFields());
        assertFalse(processor.isIgnoreMissing());
    }

    public void testIgnoreMissing() {
        Map<String, Object> config = new HashMap<>();
        List<String> fieldList = new ArrayList<>();
        fieldList.add("_field");
        config.put("fields", fieldList);
        config.put("ignore_missing", true);

        String processorTag = randomAlphaOfLength(10);
        FingerprintProcessor processor = factory.create(null, processorTag, config);
        assertEquals(processorTag, processor.getTag());
        assertEquals(FingerprintProcessor.TYPE, processor.getType());
        Set<String> expectFeilds = new HashSet<>();
        expectFeilds.add("_field");
        assertEquals(expectFeilds, processor.getFields());
        assertEquals("fingerprint", processor.getTargetField());
        assertEquals(FingerprintProcessor.Method.MD5, processor.getMethod());
        assertFalse(processor.isBase64Encode());
        assertFalse(processor.isConcatenateAllFields());
        assertTrue(processor.isIgnoreMissing());

    }
}
