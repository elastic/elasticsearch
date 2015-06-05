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
package org.elasticsearch.test.rest.section;

import com.google.common.collect.Maps;
import org.elasticsearch.test.rest.RestTestExecutionContext;

import java.io.IOException;
import java.util.Map;

/**
 * Represents a set section:
 *
 *   - set: {_scroll_id: scroll_id}
 *
 */
public class SetSection implements ExecutableSection {

    private Map<String, String> stash = Maps.newHashMap();

    public void addSet(String responseField, String stashedField) {
        stash.put(responseField, stashedField);
    }

    public Map<String, String> getStash() {
        return stash;
    }

    @Override
    public void execute(RestTestExecutionContext executionContext) throws IOException {
        for (Map.Entry<String, String> entry : stash.entrySet()) {
            Object actualValue = executionContext.response(entry.getKey());
            executionContext.stash().stashValue(entry.getValue(), actualValue);
        }
    }
}
