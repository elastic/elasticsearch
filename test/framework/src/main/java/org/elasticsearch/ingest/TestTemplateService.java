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

import java.util.Map;

public class TestTemplateService implements TemplateService {
    private boolean compilationException;

    public static TemplateService instance() {
        return new TestTemplateService(false);
    }

    public static TemplateService instance(boolean compilationException) {
        return new TestTemplateService(compilationException);
    }

    private TestTemplateService(boolean compilationException) {
        this.compilationException = compilationException;
    }

    @Override
    public Template compile(String template) {
        if (this.compilationException) {
            throw new RuntimeException("could not compile script");
        } else {
            return new MockTemplate(template);
        }
    }

    public static class MockTemplate implements TemplateService.Template {

        private final String expected;

        public MockTemplate(String expected) {
            this.expected = expected;
        }

        @Override
        public String execute(Map<String, Object> model) {
            return expected;
        }

        @Override
        public String getKey() {
            return expected;
        }
    }
}
