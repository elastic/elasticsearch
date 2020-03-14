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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Helper to build {@link org.elasticsearch.rest.action.cat.RestTable} display attributes
 */
public final class TableColumnAttributeBuilder {

    private static final String ATTR_DELIM = ";";
    private String aliases;
    private Boolean displayByDefault;
    private String description;
    private String textAlignment;
    private String sibling;

    public static TableColumnAttributeBuilder builder() {
        return new TableColumnAttributeBuilder();
    }

    public static TableColumnAttributeBuilder builder(String description) {
        return new TableColumnAttributeBuilder().setDescription(description);
    }

    public static TableColumnAttributeBuilder builder(String description, boolean display) {
        return new TableColumnAttributeBuilder().setDescription(description).setDisplayByDefault(display);
    }

    /**
     * Set the various aliases available to this column
     *
     * The API consumer can supply one of these strings in the parameter flags instead of the full column name
     *
     * Default: no configured aliases
     * @param aliases Non-null array of strings
     */
    public TableColumnAttributeBuilder setAliases(String... aliases) {
        this.aliases = Strings.arrayToCommaDelimitedString(aliases);
        return this;
    }

    /**
     * Whether or not to display the column by default
     *
     * Default: true, always display the column
     * @param displayByDefault When false, the user must specify to display the column in the REST request
     */
    public TableColumnAttributeBuilder setDisplayByDefault(Boolean displayByDefault) {
        this.displayByDefault = displayByDefault;
        return this;
    }

    /**
     * Human readable description of the column. Consumed via the `help` REST call.
     *
     * Default: "not available"
     * @param description Human readable description
     */
    public TableColumnAttributeBuilder setDescription(String description) {
        this.description = description;
        return this;
    }

    /**
     * Text alignment for the column when building the table
     *
     * Default: {@link TextAlign#LEFT}
     * @param textAlignment The desired text alignment
     */
    public TableColumnAttributeBuilder setTextAlignment(TextAlign textAlignment) {
        this.textAlignment = textAlignment.toString();
        return this;
    }

    /**
     * The sibling column for this column
     *
     * If a normal column is included and the sibling is requested, then the sibling is included as well
     *
     * Example:
     *
     * Cat indices, requesting `pri` and wanting the `cs` columns (which have a sibling, pri.cs)
     *
     * {@code GET _cat/indices?pri&h=cs&v}
     *
     *       ss   pri.ss
     *    290kb    290kb
     *     230b     230b
     *
     * Default: No sibling
     * @param sibling The sibling column prefix
     */
    public TableColumnAttributeBuilder setSibling(String sibling) {
        this.sibling = sibling;
        return this;
    }

    public String build() {
        List<String> attrs = new ArrayList<>();
        if (aliases != null) {
            attrs.add("alias:" + aliases);
        }
        if (displayByDefault != null) {
            attrs.add("default:" + displayByDefault.toString());
        }
        if (description != null) {
            attrs.add("desc:" + description);
        }
        if (textAlignment != null) {
            attrs.add("text-align:" + textAlignment);
        }
        if (sibling != null) {
            attrs.add("sibling:" + sibling);
        }
        return String.join(ATTR_DELIM, attrs);
    }

    public enum TextAlign {
        LEFT,
        RIGHT,
        NONE;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

}
