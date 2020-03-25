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

package org.elasticsearch.dissect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents the matches of a {@link DissectParser#parse(String)}. Handles the appending and referencing based on the key instruction.
 */
final class DissectMatch {

    private final String appendSeparator;
    private final Map<String, String> results;
    private final Map<String, String> simpleResults;
    private final Map<String, ReferenceResult> referenceResults;
    private final Map<String, AppendResult> appendResults;
    private int implicitAppendOrder = -1000;
    private final int maxMatches;
    private final int maxResults;
    private final int appendCount;
    private final int referenceCount;
    private final int simpleCount;
    private int matches = 0;

    DissectMatch(String appendSeparator, int maxMatches, int maxResults, int appendCount, int referenceCount) {
        if (maxMatches <= 0 || maxResults <= 0) {
            throw new IllegalArgumentException("Expected results are zero, can not construct DissectMatch");//should never happen
        }
        this.maxMatches = maxMatches;
        this.maxResults = maxResults;
        this.appendCount = appendCount;
        this.referenceCount = referenceCount;
        this.appendSeparator = appendSeparator;
        results = new HashMap<>(maxResults);
        this.simpleCount = maxMatches - referenceCount - appendCount;
        simpleResults = simpleCount <= 0 ? null : new HashMap<>(simpleCount);
        referenceResults = referenceCount <= 0 ? null : new HashMap<>(referenceCount);
        appendResults = appendCount <= 0 ? null : new HashMap<>(appendCount);
    }

    /**
     * Add the key/value that was found as result of the parsing
     * @param key the {@link DissectKey}
     * @param value the discovered value for the key
     */
    void add(DissectKey key, String value) {
        matches++;
        if (key.skip()) {
            return;
        }
        switch (key.getModifier()) {
            case NONE:
                simpleResults.put(key.getName(), value);
                break;
            case APPEND:
                appendResults.computeIfAbsent(key.getName(), k -> new AppendResult(appendSeparator)).addValue(value, implicitAppendOrder++);
                break;
            case APPEND_WITH_ORDER:
                appendResults.computeIfAbsent(key.getName(),
                    k -> new AppendResult(appendSeparator)).addValue(value, key.getAppendPosition());
                break;
            case FIELD_NAME:
                referenceResults.computeIfAbsent(key.getName(), k -> new ReferenceResult()).setKey(value);
                break;
            case FIELD_VALUE:
                referenceResults.computeIfAbsent(key.getName(), k -> new ReferenceResult()).setValue(value);
                break;
        }
    }

    boolean fullyMatched() {
        return matches == maxMatches;
    }

    /**
     * Checks if results are valid.
     * @param results the results to check
     * @return true if all dissect keys have been matched and the results are of the expected size.
     */
    boolean isValid(Map<String, String> results) {
        return fullyMatched() && results.size() == maxResults;
    }

    /**
     * Gets all the current matches. Pass the results of this to isValid to determine if a fully successful match has occured.
     *
     * @return the map of the results.
     */
    Map<String, String> getResults() {
        results.clear();
        if (simpleCount > 0) {
            results.putAll(simpleResults);
        }
        if (referenceCount > 0) {
            referenceResults.forEach((k, v) -> results.put(v.getKey(), v.getValue()));
        }
        if (appendCount > 0) {
            appendResults.forEach((k, v) -> results.put(k, v.getAppendResult()));
        }

        return results;
    }

    /**
     * a result that will need to be part of an append operation.
     */
    private final class AppendResult {
        private final List<AppendValue> values = new ArrayList<>();
        private final String appendSeparator;

        private AppendResult(String appendSeparator) {
            this.appendSeparator = appendSeparator;
        }

        private void addValue(String value, int order) {
            values.add(new AppendValue(value, order));
        }

        private String getAppendResult() {
            Collections.sort(values);
            return values.stream().map(AppendValue::getValue).collect(Collectors.joining(appendSeparator));
        }
    }

    /**
     * An appendable value that can be sorted based on the provided order
     */
    private final class AppendValue implements Comparable<AppendValue> {
        private final String value;
        private final int order;

        private AppendValue(String value, int order) {
            this.value = value;
            this.order = order;
        }

        private String getValue() {
            return value;
        }

        private int getOrder() {
            return order;
        }

        @Override
        public int compareTo(AppendValue o) {
            return Integer.compare(this.order, o.getOrder());
        }
    }

    /**
     * A result that needs to be converted to a key/value reference
     */
    private final class ReferenceResult {

        private String key;

        private String getKey() {
            return key;
        }

        private String getValue() {
            return value;
        }

        private String value;

        private void setValue(String value) {
            this.value = value;
        }

        private void setKey(String key) {
            this.key = key;
        }
    }
}
