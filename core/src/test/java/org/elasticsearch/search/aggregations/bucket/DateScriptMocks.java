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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptModule;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mock scripts shared by DateRangeIT and DateHistogramIT
 */
public class DateScriptMocks {

    /**
     * Mock plugin for the {@link DateScriptMocks.ExtractFieldScript} and {@link DateScriptMocks.PlusOneMonthScript}
     */
    public static class DateScriptsMockPlugin extends Plugin implements ScriptPlugin {
        @Override
        public List<NativeScriptFactory> getNativeScripts() {
            return Arrays.asList(new ExtractFieldScriptFactory(), new PlusOneMonthScriptFactory());
        }
    }

    public static class ExtractFieldScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new ExtractFieldScript((String) params.get("fieldname"));
        }
        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public String getName() {
            return ExtractFieldScript.NAME;
        }
    }

    public static class ExtractFieldScript extends AbstractSearchScript {

        public static final String NAME = "extract_field";
        private String fieldname;

        public ExtractFieldScript(String fieldname) {
            this.fieldname  = fieldname;
        }

        @Override
        public Object run() {
            return doc().get(fieldname);
        }
    }

    public static class PlusOneMonthScriptFactory implements NativeScriptFactory {

        @Override
        public ExecutableScript newScript(Map<String, Object> params) {
            return new PlusOneMonthScript((String) params.get("fieldname"));
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public String getName() {
            return PlusOneMonthScript.NAME;
        }
    }

    /**
     * This mock script takes date field value and adds one month to the returned date
     */
    public static class PlusOneMonthScript extends AbstractSearchScript {

        public static final String NAME = "date_plus_1_month";
        private String fieldname;

        private Map<String, Object> vars = new HashMap<>();

        public PlusOneMonthScript(String fieldname) {
            this.fieldname  = fieldname;
        }

        @Override
        public void setNextVar(String name, Object value) {
            vars.put(name, value);
        }

        @Override
        public long runAsLong() {
            return new DateTime((long) vars.get("_value"), DateTimeZone.UTC).plusMonths(1).getMillis();
        }

        @Override
        public double runAsDouble() {
            return new DateTime(Double.valueOf((double) vars.get("_value")).longValue(), DateTimeZone.UTC).plusMonths(1).getMillis();
        }

        @Override
        public Object run() {
            return new UnsupportedOperationException();
        }
    }
}
