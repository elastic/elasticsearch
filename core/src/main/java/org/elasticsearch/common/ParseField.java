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
package org.elasticsearch.common;

import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Holds a field that can be found in a request while parsing and its different variants, which may be deprecated.
 */
public class ParseField {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(ParseField.class));

    private final String camelCaseName;
    private final String underscoreName;
    private final String[] deprecatedNames;
    private String allReplacedWith = null;
    private final String[] allNames;

    public ParseField(String value, String... deprecatedNames) {
        camelCaseName = Strings.toCamelCase(value);
        underscoreName = Strings.toUnderscoreCase(value);
        if (deprecatedNames == null || deprecatedNames.length == 0) {
            this.deprecatedNames = Strings.EMPTY_ARRAY;
        } else {
            final HashSet<String> set = new HashSet<>();
            for (String depName : deprecatedNames) {
                set.add(Strings.toCamelCase(depName));
                set.add(Strings.toUnderscoreCase(depName));
            }
            this.deprecatedNames = set.toArray(new String[set.size()]);
        }
        Set<String> allNames = new HashSet<>();
        allNames.add(camelCaseName);
        allNames.add(underscoreName);
        Collections.addAll(allNames, this.deprecatedNames);
        this.allNames = allNames.toArray(new String[allNames.size()]);
    }

    public String getPreferredName(){
        return underscoreName;
    }

    public String[] getAllNamesIncludedDeprecated() {
        return allNames;
    }

    public ParseField withDeprecation(String... deprecatedNames) {
        return new ParseField(this.underscoreName, deprecatedNames);
    }

    /**
     * Return a new ParseField where all field names are deprecated and replaced with {@code allReplacedWith}.
     */
    public ParseField withAllDeprecated(String allReplacedWith) {
        ParseField parseField = this.withDeprecation(getAllNamesIncludedDeprecated());
        parseField.allReplacedWith = allReplacedWith;
        return parseField;
    }

    boolean match(String currentFieldName, boolean strict) {
        if (allReplacedWith == null && (currentFieldName.equals(camelCaseName) || currentFieldName.equals(underscoreName))) {
            return true;
        }
        String msg;
        for (String depName : deprecatedNames) {
            if (currentFieldName.equals(depName)) {
                msg = "Deprecated field [" + currentFieldName + "] used, expected [" + underscoreName + "] instead";
                if (allReplacedWith != null) {
                    msg = "Deprecated field [" + currentFieldName + "] used, replaced by [" + allReplacedWith + "]";
                }
                if (strict) {
                    throw new IllegalArgumentException(msg);
                } else {
                    DEPRECATION_LOGGER.deprecated(msg);
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return getPreferredName();
    }

    public String getAllReplacedWith() {
        return allReplacedWith;
    }

    public String getCamelCaseName() {
        return camelCaseName;
    }

    public String[] getDeprecatedNames() {
        return deprecatedNames;
    }
}
