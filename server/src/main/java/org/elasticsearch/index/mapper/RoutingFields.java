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
 * Collects fields contributing to routing from documents.
 */
public interface RoutingFields {

    /**
     * Collect routing fields from index settings
     */
    static RoutingFields fromIndexSettings(IndexSettings indexSettings) {
        return indexSettings.getMode().buildRoutingFields(indexSettings);
    }

    /**
     * This overloaded method tries to take advantage of the fact that the UTF-8
     * value is already computed in some cases when we want to collect
     * routing fields, so we can save re-computing the UTF-8 encoding.
     */
    RoutingFields addString(String fieldName, BytesRef utf8Value);

    default RoutingFields addString(String fieldName, String value) {
        return addString(fieldName, new BytesRef(value));
    }

    RoutingFields addIp(String fieldName, InetAddress value);

    RoutingFields addLong(String fieldName, long value);

    RoutingFields addUnsignedLong(String fieldName, long value);

    RoutingFields addBoolean(String fieldName, boolean value);

    /**
     * Noop implementation that doesn't perform validations on routing fields
     */
    enum Noop implements RoutingFields {

        INSTANCE;

        @Override
        public RoutingFields addString(String fieldName, BytesRef utf8Value) {
            return this;
        }

        @Override
        public RoutingFields addString(String fieldName, String value) {
            return this;
        }

        @Override
        public RoutingFields addIp(String fieldName, InetAddress value) {
            return this;
        }

        @Override
        public RoutingFields addLong(String fieldName, long value) {
            return this;
        }

        @Override
        public RoutingFields addUnsignedLong(String fieldName, long value) {
            return this;
        }

        @Override
        public RoutingFields addBoolean(String fieldName, boolean value) {
            return this;
        }
    }
}
