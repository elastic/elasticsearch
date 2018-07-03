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

package org.elasticsearch.index.fielddata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class ScriptDocValuesJodaBwcTests extends ScriptDocValuesDatesTests {

    @Override
    public void test() throws IOException {
        assertDateDocValues(millis -> new DateTime(millis, DateTimeZone.UTC));
    }

    public void testDeprecationWarning() {
        new ScriptModule(Settings.EMPTY, Collections.singletonList(new ScriptPlugin() {
            @Override
            public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
                return new MockScriptEngine(MockScriptEngine.NAME, Collections.singletonMap("1", script -> "1"));
            }
        }));
        assertWarnings("The joda time api for doc values is deprecated. Use -Des.script.use_java_time=true" +
                       "to use the java time api for date field doc values");
    }
}
