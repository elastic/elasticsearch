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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DateIndexNameFactoryTests extends ESTestCase {

    public void testDefaults() throws Exception {
        DateIndexNameProcessor.Factory factory = new DateIndexNameProcessor.Factory(TestTemplateService.instance());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("date_rounding", "y");

        DateIndexNameProcessor processor = factory.create(null, null, config);
        assertThat(processor.getDateFormats().size(), Matchers.equalTo(1));
        assertThat(processor.getField(), Matchers.equalTo("_field"));
        assertThat(processor.getIndexNamePrefixTemplate().newInstance(Collections.emptyMap()).execute(), Matchers.equalTo(""));
        assertThat(processor.getDateRoundingTemplate().newInstance(Collections.emptyMap()).execute(), Matchers.equalTo("y"));
        assertThat(processor.getIndexNameFormatTemplate().newInstance(Collections.emptyMap()).execute(), Matchers.equalTo("yyyy-MM-dd"));
        assertThat(processor.getTimezone(), Matchers.equalTo(ZoneOffset.UTC));
    }

    public void testSpecifyOptionalSettings() throws Exception {
        DateIndexNameProcessor.Factory factory = new DateIndexNameProcessor.Factory(TestTemplateService.instance());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("index_name_prefix", "_prefix");
        config.put("date_rounding", "y");
        config.put("date_formats", Arrays.asList("UNIX", "UNIX_MS"));

        DateIndexNameProcessor processor = factory.create(null, null, config);
        assertThat(processor.getDateFormats().size(), Matchers.equalTo(2));

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("index_name_prefix", "_prefix");
        config.put("date_rounding", "y");
        config.put("index_name_format", "yyyyMMdd");

        processor = factory.create(null, null, config);
        assertThat(processor.getIndexNameFormatTemplate().newInstance(Collections.emptyMap()).execute(), Matchers.equalTo("yyyyMMdd"));

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("index_name_prefix", "_prefix");
        config.put("date_rounding", "y");
        config.put("timezone", "+02:00");

        processor = factory.create(null, null, config);
        assertThat(processor.getTimezone(), Matchers.equalTo(ZoneOffset.ofHours(2)));

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("index_name_prefix", "_prefix");
        config.put("date_rounding", "y");

        processor = factory.create(null, null, config);
        assertThat(processor.getIndexNamePrefixTemplate().newInstance(Collections.emptyMap()).execute(), Matchers.equalTo("_prefix"));
    }

    public void testRequiredFields() throws Exception {
        DateIndexNameProcessor.Factory factory = new DateIndexNameProcessor.Factory(TestTemplateService.instance());
        Map<String, Object> config = new HashMap<>();
        config.put("date_rounding", "y");
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, config));
        assertThat(e.getMessage(), Matchers.equalTo("[field] required property is missing"));

        config.clear();
        config.put("field", "_field");
        e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, config));
        assertThat(e.getMessage(), Matchers.equalTo("[date_rounding] required property is missing"));
    }
}
