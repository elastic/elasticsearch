/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * Collects dimensions from documents.
 */
public interface DocumentDimensions {
    void addString(String fieldName, String value);

    void addIp(String fieldName, InetAddress value);

    void addLong(String fieldName, long value);

    void addUnsignedLong(String fieldName, long value);

    /**
     * Makes sure that each dimension only appears on time.
     */
    class OnlySingleValueAllowed implements DocumentDimensions {
        private final Set<String> names = new HashSet<>();

        @Override
        public void addString(String fieldName, String value) {
            add(fieldName);
        }

        @Override
        public void addIp(String fieldName, InetAddress value) {
            add(fieldName);
        }

        @Override
        public void addLong(String fieldName, long value) {
            add(fieldName);
        }

        @Override
        public void addUnsignedLong(String fieldName, long value) {
            add(fieldName);
        }

        private void add(String fieldName) {
            boolean isNew = names.add(fieldName);
            if (false == isNew) {
                throw new IllegalArgumentException("Dimension field [" + fieldName + "] cannot be a multi-valued field.");
            }
        }
    };
}
