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

import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.util.EnumSet;
import java.util.HashSet;

/**
 */
public class ParseField {
    private final String camelCaseName;
    private final String underscoreName;
    private final String[] deprecatedNames;
    private String allReplacedWith = null;

    public static final EnumSet<Flag> EMPTY_FLAGS = EnumSet.noneOf(Flag.class);

    public static enum Flag {
        STRICT
    }

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
            this.deprecatedNames = set.toArray(new String[0]);
        }
    }

    public String getPreferredName(){
        return underscoreName;
    }

    public String[] getAllNamesIncludedDeprecated() {
        String[] allNames = new String[2 + deprecatedNames.length];
        allNames[0] = camelCaseName;
        allNames[1] = underscoreName;
        for (int i = 0; i < deprecatedNames.length; i++) {
            allNames[i + 2] = deprecatedNames[i];
        }
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

    public boolean match(String currentFieldName) {
        return match(currentFieldName, EMPTY_FLAGS);
    }
    
    public boolean match(String currentFieldName, EnumSet<Flag> flags) {
        if (allReplacedWith == null && (currentFieldName.equals(camelCaseName) || currentFieldName.equals(underscoreName))) {
            return true;
        }
        String msg;
        for (String depName : deprecatedNames) {
            if (currentFieldName.equals(depName)) {
                if (flags.contains(Flag.STRICT)) {
                    msg = "Deprecated field [" + currentFieldName + "] used, expected [" + underscoreName + "] instead";
                    if (allReplacedWith != null) {
                        msg = "Deprecated field [" + currentFieldName + "] used, replaced by [" + allReplacedWith + "]";
                    }
                    throw new ElasticsearchIllegalArgumentException(msg);
                }
                return true;
            }
        }
        return false;
    }

}
