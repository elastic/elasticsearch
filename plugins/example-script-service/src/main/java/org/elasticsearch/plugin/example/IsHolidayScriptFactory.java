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

package org.elasticsearch.plugin.example;

import de.jollyday.HolidayCalendar;
import de.jollyday.HolidayManager;
import de.jollyday.ManagerParameters;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.joda.time.ReadableDateTime;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.LocalDate;
import java.util.Map;

/**
 * An example of a script that returns true for holidays and false for work days in different countries
 */
public class IsHolidayScriptFactory implements ExampleScriptFactory {
    @Override
    public SearchScript search(SearchLookup lookup, @Nullable Map<String, Object> vars) {
        String dateFieldName = vars == null ? null : XContentMapValues.nodeStringValue(vars.get("field"), "timestamp");
        String country = vars == null ? null : XContentMapValues.nodeStringValue(vars.get("country"), "UNITED_STATES");

        // Loading of holiday list requires special permissions that needs to be listed in src/main/plugin-metadata/plugin-security.policy
        HolidayManager holidayManager = AccessController.doPrivileged(new PrivilegedAction<HolidayManager>() {
            public HolidayManager run() {
                ClassLoader contextClassLoader = null;
                try {
                    //TODO: figure out how to make it cleaner
                    // HolidayManager manager is loading internal resources from its classloader
                    contextClassLoader = Thread.currentThread().getContextClassLoader();
                    if (contextClassLoader != null) {
                        Thread.currentThread().setContextClassLoader(HolidayManager.class.getClassLoader());
                    }
                    return HolidayManager.getInstance(ManagerParameters.create(HolidayCalendar.valueOf(country)));
                }finally {
                    if (contextClassLoader != null) {
                        Thread.currentThread().setContextClassLoader(contextClassLoader);
                    }
                }
            }
        });

        final AbstractSearchScript script = new AbstractSearchScript() {
            @Override
            public Object run() {
                ScriptDocValues.Longs dateAsLong = docFieldLongs(dateFieldName);
                ReadableDateTime date = dateAsLong.getDate();
                LocalDate localDate = LocalDate.of(date.getYear(), date.getMonthOfYear(), date.getDayOfMonth());
                return holidayManager.isHoliday(localDate);
            }
        };
        return new ExampleSearchScript(lookup, script, false);
    }

}
