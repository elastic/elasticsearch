/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexSettings;

import java.net.InetAddress;

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
    DocumentDimensions addString(String fieldName, BytesRef utf8Value);

    default DocumentDimensions addString(String fieldName, String value) {
        return addString(fieldName, new BytesRef(value));
    }

    DocumentDimensions addIp(String fieldName, InetAddress value);

    DocumentDimensions addLong(String fieldName, long value);

    DocumentDimensions addUnsignedLong(String fieldName, long value);

    DocumentDimensions addBoolean(String fieldName, boolean value);

    DocumentDimensions validate(IndexSettings settings);

    /**
     * Noop implementation that doesn't perform validations on dimension fields
     */
    enum Noop implements DocumentDimensions {

        INSTANCE;

        @Override
        public DocumentDimensions addString(String fieldName, BytesRef utf8Value) {
            return this;
        }

        @Override
        public DocumentDimensions addString(String fieldName, String value) {
            return this;
        }

        @Override
        public DocumentDimensions addIp(String fieldName, InetAddress value) {
            return this;
        }

        @Override
        public DocumentDimensions addLong(String fieldName, long value) {
            return this;
        }

        @Override
        public DocumentDimensions addUnsignedLong(String fieldName, long value) {
            return this;
        }

        @Override
        public DocumentDimensions addBoolean(String fieldName, boolean value) {
            return this;
        }

        @Override
        public DocumentDimensions validate(IndexSettings settings) {
            return this;
        }
    }
}
