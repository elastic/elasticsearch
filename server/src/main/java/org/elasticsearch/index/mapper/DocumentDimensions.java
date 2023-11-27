/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexSettings;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * Collects dimensions from documents.
 */
public interface DocumentDimensions {

    /**
     * Build an index's DocumentDimensions using its settings
     */
    static DocumentDimensions fromIndexSettings(IndexSettings indexSettings) {
        return indexSettings.getMode().buildDocumentDimensions(indexSettings);
    }

    /**
     * This overloaded method tries to take advantage of the fact that the UTF-8
     * value is already computed in some cases when we want to collect
     * dimensions, so we can save re-computing the UTF-8 encoding.
     */
    void addString(String fieldName, BytesRef utf8Value);

    default void addString(String fieldName, String value) {
        addString(fieldName, new BytesRef(value));
    }

    void addIp(String fieldName, InetAddress value);

    void addLong(String fieldName, long value);

    void addUnsignedLong(String fieldName, long value);

    /**
     * Makes sure that each dimension only appears on time.
     */
    class OnlySingleValueAllowed implements DocumentDimensions {
        private final Set<String> names = new HashSet<>();

        @Override
        public void addString(String fieldName, BytesRef value) {
            add(fieldName);
        }

        // Override to skip the UTF-8 conversion that happens in the default implementation
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
