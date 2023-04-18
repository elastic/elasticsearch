/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.common.http;

import org.elasticsearch.xcontent.XContentType;

import java.util.Locale;

import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalArgument;

public enum HttpContentType {

    JSON() {
        @Override
        public XContentType contentType() {
            return XContentType.JSON;
        }
    },

    YAML() {
        @Override
        public XContentType contentType() {
            return XContentType.YAML;
        }
    },

    TEXT() {
        @Override
        public XContentType contentType() {
            return null;
        }
    };

    public abstract XContentType contentType();

    @Override
    public String toString() {
        return id();
    }

    public String id() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static HttpContentType resolve(String id) {
        return switch (id.toLowerCase(Locale.ROOT)) {
            case "json" -> JSON;
            case "yaml" -> YAML;
            case "text" -> TEXT;
            default -> throw illegalArgument("unknown http content type [{}]", id);
        };
    }
}
