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

public class HtmlStripProcessorTests extends AbstractStringProcessorTestCase<String> {

    @Override
    protected AbstractStringProcessor<String> newProcessor(String field, boolean ignoreMissing, String targetField) {
        return new HtmlStripProcessor(randomAlphaOfLength(10), null, field, ignoreMissing, targetField);
    }

    @Override
    protected String modifyInput(String input) {
        return "<p><b>test</b>" + input + "<p><b>test</b>";
    }

    @Override
    protected String expectedResult(String input) {
        return "\ntest" + input + "\ntest";
    }
}
