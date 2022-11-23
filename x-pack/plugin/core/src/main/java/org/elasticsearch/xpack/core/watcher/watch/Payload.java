/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.watch;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.watcher.support.WatcherUtils.responseToData;

public interface Payload extends ToXContentObject {

    Simple EMPTY = new Simple(Collections.emptyMap());

    Map<String, Object> data();

    class Simple implements Payload {

        private final Map<String, Object> data;

        public Simple() {
            this(new HashMap<>());
        }

        public Simple(String key, Object value) {
            this(new MapBuilder<String, Object>().put(key, value).map());
        }

        public Simple(Map<String, Object> data) {
            this.data = data;
        }

        @Override
        public Map<String, Object> data() {
            return data;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.map(data);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Simple simple = (Simple) o;

            if (data.equals(simple.data) == false) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return data.hashCode();
        }

        @Override
        public String toString() {
            return "simple[" + Objects.toString(data) + "]";
        }
    }

    class XContent extends Simple {
        public XContent(ToXContentObject response, Params params) throws IOException {
            super(responseToData(response, params));
        }
    }
}
