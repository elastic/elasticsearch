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

package org.elasticsearch.painless;

import org.elasticsearch.script.JodaCompatibleZonedDateTime;

import java.time.ZonedDateTime;

/**
 * A set of methods for non-native boxing and non-native
 * exact math operations used at both compile-time and runtime.
 */
public class Utility {

    public static String charToString(final char value) {
        return String.valueOf(value);
    }

    public static char StringTochar(final String value) {
        if (value == null) {
            throw new ClassCastException("cannot cast " +
                    "null " + String.class.getCanonicalName() +  " to " + char.class.getCanonicalName());
        }

        if (value.length() != 1) {
            throw new ClassCastException("cannot cast " +
                    String.class.getCanonicalName() +  " with length not equal to one to " + char.class.getCanonicalName());
        }

        return value.charAt(0);
    }

    // TODO: remove this when the transition from Joda to Java datetimes is completed
    public static ZonedDateTime JCZDTToZonedDateTime(final JodaCompatibleZonedDateTime jczdt) {
        return jczdt.getZonedDateTime();
    }

    private Utility() {}
}
