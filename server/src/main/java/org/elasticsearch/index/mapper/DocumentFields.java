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
public interface DocumentFields {

    /**
     * Build an index's DocumentDimensions using its settings
     */
    static DocumentFields fromIndexSettings(IndexSettings indexSettings) {
        return indexSettings.getMode().buildDocumentDimensions(indexSettings);
    }

    /**
     * This overloaded method tries to take advantage of the fact that the UTF-8
     * value is already computed in some cases when we want to collect
     * dimensions, so we can save re-computing the UTF-8 encoding.
     */
    void addKeywordDimension(String fieldName, BytesRef utf8Value);

    default void addKeywordDimension(String fieldName, String value) {
        addKeywordDimension(fieldName, new BytesRef(value));
    }

    void addIpDimension(String fieldName, InetAddress value);

    void addLongDimension(String fieldName, long value);

    void addUnsignedLongDimension(String fieldName, long value);

    /**
     * Makes sure that each dimension only appears on time.
     */
    class OnlySingleValueAllowed implements DocumentFields {
        private final Set<String> names = new HashSet<>();

        @Override
        public void addKeywordDimension(String fieldName, BytesRef value) {
            add(fieldName);
        }

        // Override to skip the UTF-8 conversion that happens in the default implementation
        @Override
        public void addKeywordDimension(String fieldName, String value) {
            add(fieldName);
        }

        @Override
        public void addIpDimension(String fieldName, InetAddress value) {
            add(fieldName);
        }

        @Override
        public void addLongDimension(String fieldName, long value) {
            add(fieldName);
        }

        @Override
        public void addUnsignedLongDimension(String fieldName, long value) {
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
