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

package org.elasticsearch.ingest.processor.mutate;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MutateProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> update = new HashMap<>();
        update.put("foo", 123);
        config.put("update", update);
        MutateProcessor processor = factory.create(config);
        assertThat(processor.getUpdate(), equalTo(update));
    }

    public void testCreateGsubPattern() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        Map<String, List<String>> gsub = new HashMap<>();
        gsub.put("foo", Arrays.asList("\\s.*e\\s", "<word_ending_with_e>"));
        config.put("gsub", gsub);

        Map<String, Tuple<Pattern, String>> compiledGsub = new HashMap<>();
        Pattern searchPattern = Pattern.compile("\\s.*e\\s");
        compiledGsub.put("foo", new Tuple<>(searchPattern, "<word_ending_with_e>"));

        MutateProcessor processor = factory.create(config);
        for (Map.Entry<String, Tuple<Pattern, String>> entry : compiledGsub.entrySet()) {
            Tuple<Pattern, String> actualSearchAndReplace = processor.getGsub().get(entry.getKey());
            assertThat(actualSearchAndReplace, notNullValue());
            assertThat(actualSearchAndReplace.v1().pattern(), equalTo(entry.getValue().v1().pattern()));
            assertThat(actualSearchAndReplace.v2(), equalTo(entry.getValue().v2()));
        }
    }

    public void testCreateGsubPattern_InvalidFormat() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        Map<String, List<String>> gsub = new HashMap<>();
        gsub.put("foo", Arrays.asList("only_one"));
        config.put("gsub", gsub);

        try {
            factory.create(config);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Invalid search and replace values ([only_one]) for field: foo"));
        }
    }

}
