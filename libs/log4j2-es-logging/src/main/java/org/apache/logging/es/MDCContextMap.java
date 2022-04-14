/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.logging.es;

import org.apache.logging.log4j.spi.CleanableThreadContextMap;
import org.apache.logging.log4j.util.StringMap;

import java.util.Map;

public class MDCContextMap implements CleanableThreadContextMap {
    @Override
    public void removeAll(Iterable<String> keys) {

    }

    @Override
    public void putAll(Map<String, String> map) {

    }

    @Override
    public StringMap getReadOnlyContextData() {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public boolean containsKey(String key) {
        return false;
    }

    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public Map<String, String> getCopy() {
        return null;
    }

    @Override
    public Map<String, String> getImmutableMapOrNull() {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public void put(String key, String value) {

    }

    @Override
    public void remove(String key) {

    }
}
