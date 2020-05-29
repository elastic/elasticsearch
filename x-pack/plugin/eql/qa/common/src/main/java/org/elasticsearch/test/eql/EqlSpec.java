/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.test.eql;

import org.elasticsearch.common.Strings;

import java.util.Arrays;
import java.util.Objects;

public class EqlSpec {
    private String description;
    private String note;
    private String[] tags;
    private String query;
    private Boolean caseSensitiveOnly;
    private Boolean caseInsensitiveOnly;
    private long[] expectedEventIds;

    public String description() {
        return description;
    }

    public void description(String description) {
        this.description = description;
    }

    public String note() {
        return note;
    }

    public void note(String note) {
        this.note = note;
    }

    public String[] tags() {
        return tags;
    }

    public void tags(String[] tags) {
        this.tags = tags;
    }

    public String query() {
        return query;
    }

    public void query(String query) {
        this.query = query;
    }

    public long[] expectedEventIds() {
        return expectedEventIds;
    }

    public void expectedEventIds(long[] expectedEventIds) {
        this.expectedEventIds = expectedEventIds;
    }

    public void caseSensitiveOnly(Boolean caseSensitiveOnly) {
        this.caseSensitiveOnly = caseSensitiveOnly;
    }

    public void caseInsensitiveOnly(Boolean caseInsensitiveOnly) {
        this.caseInsensitiveOnly = caseInsensitiveOnly;
    }

    public boolean supportsCaseSensitive() {
        return this.caseInsensitiveOnly == null || this.caseInsensitiveOnly == false;
    }

    public boolean supportsCaseInsensitive() {
        return this.caseSensitiveOnly == null || this.caseSensitiveOnly == false;
    }

    @Override
    public String toString() {
        String str = "";
        str = appendWithComma(str, "query", query);
        str = appendWithComma(str, "description", description);
        str = appendWithComma(str, "note", note);

        if (caseInsensitiveOnly != null) {
            str = appendWithComma(str, "case_insensitive", caseInsensitiveOnly.toString());
        }

        if (caseSensitiveOnly != null) {
            str = appendWithComma(str, "case_sensitive", caseSensitiveOnly.toString());
        }

        if (tags != null) {
            str = appendWithComma(str, "tags", Arrays.toString(tags));
        }

        if (expectedEventIds != null) {
            str = appendWithComma(str, "expected_event_ids", Arrays.toString(expectedEventIds));
        }
        return str;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        EqlSpec that = (EqlSpec) other;

        return Objects.equals(this.query(), that.query())
                && Objects.equals(this.caseSensitiveOnly, that.caseSensitiveOnly)
                && Objects.equals(this.caseInsensitiveOnly, that.caseInsensitiveOnly);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.query, this.caseSensitiveOnly, this.caseInsensitiveOnly);
    }

    private static String appendWithComma(String str, String name, String append) {
        if (!Strings.isNullOrEmpty(append)) {
            if (!Strings.isNullOrEmpty(str)) {
                str += ", ";
            }
            str += name + ": " + append;
        }
        return str;
    }
}
