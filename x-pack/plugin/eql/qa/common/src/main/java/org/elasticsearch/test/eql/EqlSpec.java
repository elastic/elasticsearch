/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.eql;

import org.elasticsearch.common.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class EqlSpec {
    private String name;
    private String description;
    private String note;
    private String[] tags;
    private String query;
    /**
     * this is a set of possible valid results:
     * - if the query is deterministic, expectedEventIds should contain one single array of IDs representing the expected result
     * - if the query is non-deterministic, expectedEventIds can contain multiple arrays of IDs, one for each possible valid result
     */
    private List<long[]> expectedEventIds;
    private String[] joinKeys;

    private Integer size;
    private Integer maxSamplesPerKey;

    public String name() {
        return name;
    }

    public void name(String name) {
        this.name = name;
    }

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

    public List<long[]> expectedEventIds() {
        return expectedEventIds;
    }

    public void expectedEventIds(List<long[]> expectedEventIds) {
        this.expectedEventIds = expectedEventIds;
    }

    public String[] joinKeys() {
        return joinKeys;
    }

    public void joinKeys(String[] joinKeys) {
        this.joinKeys = joinKeys;
    }

    public Integer size() {
        return size;
    }

    public void size(Integer size) {
        this.size = size;
    }

    public Integer maxSamplesPerKey() {
        return maxSamplesPerKey;
    }

    public void maxSamplesPerKey(Integer maxSamplesPerKey) {
        this.maxSamplesPerKey = maxSamplesPerKey;
    }

    @Override
    public String toString() {
        String str = "";
        str = appendWithComma(str, "query", query);
        str = appendWithComma(str, "name", name);
        str = appendWithComma(str, "description", description);
        str = appendWithComma(str, "note", note);

        if (tags != null) {
            str = appendWithComma(str, "tags", Arrays.toString(tags));
        }

        if (expectedEventIds != null) {
            str = appendWithComma(
                str,
                "expected_event_ids",
                "[" + expectedEventIds.stream().map(Arrays::toString).collect(Collectors.joining(", ")) + "]"
            );
        }

        if (joinKeys != null) {
            str = appendWithComma(str, "join_keys", Arrays.toString(joinKeys));
        }
        if (size != null) {
            str = appendWithComma(str, "size", "" + size);
        }
        if (maxSamplesPerKey != null) {
            str = appendWithComma(str, "max_samples_per_key", "" + maxSamplesPerKey);
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
            && Objects.equals(size, that.size)
            && Objects.equals(maxSamplesPerKey, that.maxSamplesPerKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.query, size, maxSamplesPerKey);
    }

    private static String appendWithComma(String str, String name, String append) {
        if (Strings.isNullOrEmpty(append) == false) {
            if (Strings.isNullOrEmpty(str) == false) {
                str += ", ";
            }
            str += name + ": " + append;
        }
        return str;
    }
}
