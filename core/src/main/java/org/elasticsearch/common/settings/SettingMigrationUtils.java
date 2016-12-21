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
package org.elasticsearch.common.settings;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;

//TODO dm: Test me
// TODO #22298: Delete this class. Update call sites of #getAsBoolean() to <code>settings.getAsBoolean(String, Boolean)</code>.
public final class SettingMigrationUtils {
    // Emit logs as "Settings" which is easier to recognize by users.
    private static final Logger logger = Loggers.getLogger(Settings.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    private SettingMigrationUtils() {
        throw new AssertionError("No instances intended");
    }

    public static Boolean getAsBoolean(Version indexVersion, Settings settings, String settingKey, Boolean defaultValue) {
        if (indexVersion.before(Version.V_6_0_0_alpha1_UNRELEASED)) {
            //Only emit a warning if the setting's value is not a proper boolean
            final String value = settings.get(settingKey, "false");
            if (Booleans.isBoolean(value) == false) {
                boolean convertedValue = settings.getAsBooleanLenient(settingKey, defaultValue);
                deprecationLogger.deprecated("The value [{}] of setting [{}] is not coerced into boolean anymore. Please change " +
                    "this value to [{}].", value, settingKey, String.valueOf(convertedValue));
                return convertedValue;
            }
        }
        return settings.getAsBoolean(settingKey, defaultValue);
    }
}
