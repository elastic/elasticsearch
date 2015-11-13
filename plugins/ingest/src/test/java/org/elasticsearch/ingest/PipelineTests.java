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

import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.ingest.processor.mutate.MutateProcessor;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.*;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class PipelineTests extends ESTestCase {
    private Processor updateProcessor;
    private Processor lowercaseProcessor;
    private Pipeline pipeline;

    @Before
    public void setup() {
        Map<String, Object> update = Collections.singletonMap("foo", 123);
        List<String> lowercase = Collections.singletonList("foo");
        updateProcessor = new MutateProcessor(update, null, null, null, null, null, null, null, null, null);
        lowercaseProcessor = new MutateProcessor(null, null, null, null, null, null, null, null, null, lowercase);
        pipeline = new Pipeline("id", "description", Arrays.asList(updateProcessor, lowercaseProcessor));
    }

    public void testEquals() throws Exception {
        Pipeline other = new Pipeline(pipeline.getId(), pipeline.getDescription(), pipeline.getProcessors());
        assertThat(pipeline, equalTo(other));
    }

    public void testNotEqualsDiffId() throws Exception {
        Pipeline other = new Pipeline(pipeline.getId() + "foo", pipeline.getDescription(), pipeline.getProcessors());
        assertThat(pipeline, not(equalTo(other)));
    }

    public void testNotEqualsDiffDescription() throws Exception {
        Pipeline other = new Pipeline(pipeline.getId(), pipeline.getDescription() + "foo", pipeline.getProcessors());
        assertThat(pipeline, not(equalTo(other)));
    }

    public void testNotEqualsDiffProcessors() throws Exception {
        Pipeline other = new Pipeline(pipeline.getId(), pipeline.getDescription() + "foo", Collections.singletonList(updateProcessor));
        assertThat(pipeline, not(equalTo(other)));
    }
}
