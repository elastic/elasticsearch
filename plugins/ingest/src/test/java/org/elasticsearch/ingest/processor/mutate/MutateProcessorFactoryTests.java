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

import org.elasticsearch.test.ESTestCase;

import java.util.*;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MutateProcessorFactoryTests extends ESTestCase {

    public void testCreateUpdate() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> update = new HashMap<>();
        update.put("foo", 123);
        config.put("update", update);
        MutateProcessor processor = factory.create(config);
        assertThat(processor.getRename(), nullValue());
        assertThat(processor.getRemove(), nullValue());
        assertThat(processor.getGsub(), nullValue());
        assertThat(processor.getConvert(), nullValue());
        assertThat(processor.getJoin(), nullValue());
        assertThat(processor.getLowercase(), nullValue());
        assertThat(processor.getUppercase(), nullValue());
        assertThat(processor.getSplit(), nullValue());
        assertThat(processor.getTrim(), nullValue());
        assertThat(processor.getUpdate(), equalTo(update));
    }

    public void testCreateRename() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> rename = new HashMap<>();
        rename.put("foo", "bar");
        config.put("rename", rename);
        MutateProcessor processor = factory.create(config);
        assertThat(processor.getUpdate(), nullValue());
        assertThat(processor.getRemove(), nullValue());
        assertThat(processor.getGsub(), nullValue());
        assertThat(processor.getConvert(), nullValue());
        assertThat(processor.getJoin(), nullValue());
        assertThat(processor.getLowercase(), nullValue());
        assertThat(processor.getUppercase(), nullValue());
        assertThat(processor.getSplit(), nullValue());
        assertThat(processor.getTrim(), nullValue());
        assertThat(processor.getRename(), equalTo(rename));
    }

    public void testCreateRemove() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        List<String> remove = Collections.singletonList("foo");
        config.put("remove", remove);
        MutateProcessor processor = factory.create(config);
        assertThat(processor.getUpdate(), nullValue());
        assertThat(processor.getGsub(), nullValue());
        assertThat(processor.getConvert(), nullValue());
        assertThat(processor.getJoin(), nullValue());
        assertThat(processor.getLowercase(), nullValue());
        assertThat(processor.getUppercase(), nullValue());
        assertThat(processor.getSplit(), nullValue());
        assertThat(processor.getTrim(), nullValue());
        assertThat(processor.getRename(), nullValue());
        assertThat(processor.getRemove(), equalTo(remove));
    }

    public void testCreateConvert() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> convert = new HashMap<>();
        convert.put("foo", "integer");
        config.put("convert", convert);
        MutateProcessor processor = factory.create(config);
        assertThat(processor.getUpdate(), nullValue());
        assertThat(processor.getGsub(), nullValue());
        assertThat(processor.getJoin(), nullValue());
        assertThat(processor.getLowercase(), nullValue());
        assertThat(processor.getUppercase(), nullValue());
        assertThat(processor.getSplit(), nullValue());
        assertThat(processor.getTrim(), nullValue());
        assertThat(processor.getRename(), nullValue());
        assertThat(processor.getRemove(), nullValue());
        assertThat(processor.getConvert(), equalTo(convert));
    }

    public void testCreateJoin() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> join = new HashMap<>();
        join.put("foo", "bar");
        config.put("join", join);
        MutateProcessor processor = factory.create(config);
        assertThat(processor.getUpdate(), nullValue());
        assertThat(processor.getGsub(), nullValue());
        assertThat(processor.getConvert(), nullValue());
        assertThat(processor.getLowercase(), nullValue());
        assertThat(processor.getUppercase(), nullValue());
        assertThat(processor.getSplit(), nullValue());
        assertThat(processor.getTrim(), nullValue());
        assertThat(processor.getRename(), nullValue());
        assertThat(processor.getRemove(), nullValue());
        assertThat(processor.getJoin(), equalTo(join));
    }

    public void testCreateSplit() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> split = new HashMap<>();
        split.put("foo", "bar");
        config.put("split", split);
        MutateProcessor processor = factory.create(config);
        assertThat(processor.getUpdate(), nullValue());
        assertThat(processor.getGsub(), nullValue());
        assertThat(processor.getConvert(), nullValue());
        assertThat(processor.getLowercase(), nullValue());
        assertThat(processor.getUppercase(), nullValue());
        assertThat(processor.getJoin(), nullValue());
        assertThat(processor.getTrim(), nullValue());
        assertThat(processor.getRename(), nullValue());
        assertThat(processor.getRemove(), nullValue());
        assertThat(processor.getSplit(), equalTo(split));
    }

    public void testCreateLowercase() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        List<String> lowercase = Collections.singletonList("foo");
        config.put("lowercase", lowercase);
        MutateProcessor processor = factory.create(config);
        assertThat(processor.getUpdate(), nullValue());
        assertThat(processor.getGsub(), nullValue());
        assertThat(processor.getConvert(), nullValue());
        assertThat(processor.getJoin(), nullValue());
        assertThat(processor.getRemove(), nullValue());
        assertThat(processor.getUppercase(), nullValue());
        assertThat(processor.getSplit(), nullValue());
        assertThat(processor.getTrim(), nullValue());
        assertThat(processor.getRename(), nullValue());
        assertThat(processor.getLowercase(), equalTo(lowercase));
    }

    public void testCreateUppercase() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        List<String> uppercase = Collections.singletonList("foo");
        config.put("uppercase", uppercase);
        MutateProcessor processor = factory.create(config);
        assertThat(processor.getUpdate(), nullValue());
        assertThat(processor.getGsub(), nullValue());
        assertThat(processor.getConvert(), nullValue());
        assertThat(processor.getJoin(), nullValue());
        assertThat(processor.getRemove(), nullValue());
        assertThat(processor.getLowercase(), nullValue());
        assertThat(processor.getSplit(), nullValue());
        assertThat(processor.getTrim(), nullValue());
        assertThat(processor.getRename(), nullValue());
        assertThat(processor.getUppercase(), equalTo(uppercase));
    }

    public void testCreateTrim() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        List<String> trim = Collections.singletonList("foo");
        config.put("trim", trim);
        MutateProcessor processor = factory.create(config);
        assertThat(processor.getUpdate(), nullValue());
        assertThat(processor.getGsub(), nullValue());
        assertThat(processor.getConvert(), nullValue());
        assertThat(processor.getJoin(), nullValue());
        assertThat(processor.getRemove(), nullValue());
        assertThat(processor.getUppercase(), nullValue());
        assertThat(processor.getSplit(), nullValue());
        assertThat(processor.getLowercase(), nullValue());
        assertThat(processor.getRename(), nullValue());
        assertThat(processor.getTrim(), equalTo(trim));
    }

    public void testCreateGsubPattern() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        Map<String, List<String>> gsub = new HashMap<>();
        gsub.put("foo", Arrays.asList("\\s.*e\\s", "<word_ending_with_e>"));
        config.put("gsub", gsub);

        MutateProcessor processor = factory.create(config);
        assertThat(processor.getGsub().size(), equalTo(1));
        GsubExpression gsubExpression = processor.getGsub().get(0);
        assertThat(gsubExpression.getFieldName(), equalTo("foo"));
        assertThat(gsubExpression.getPattern().pattern(), equalTo(Pattern.compile("\\s.*e\\s").pattern()));
        assertThat(gsubExpression.getReplacement(), equalTo("<word_ending_with_e>"));
    }

    public void testCreateGsubPatternInvalidFormat() throws Exception {
        MutateProcessor.Factory factory = new MutateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        Map<String, List<String>> gsub = new HashMap<>();
        gsub.put("foo", Collections.singletonList("only_one"));
        config.put("gsub", gsub);

        try {
            factory.create(config);
            fail("processor creation should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Invalid search and replace values [only_one] for field: foo"));
        }
    }
}
