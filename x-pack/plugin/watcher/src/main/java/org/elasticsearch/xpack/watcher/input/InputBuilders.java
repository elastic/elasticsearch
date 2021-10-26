/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xpack.core.watcher.input.none.NoneInput;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.input.chain.ChainInput;
import org.elasticsearch.xpack.watcher.input.http.HttpInput;
import org.elasticsearch.xpack.watcher.input.search.SearchInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;

import java.util.HashMap;
import java.util.Map;

public final class InputBuilders {

    private InputBuilders() {
    }

    public static NoneInput.Builder noneInput() {
        return NoneInput.builder();
    }

    public static SearchInput.Builder searchInput(WatcherSearchTemplateRequest request) {
        return SearchInput.builder(request);
    }

    public static SimpleInput.Builder simpleInput() {
        return simpleInput(new HashMap<>());
    }

    public static SimpleInput.Builder simpleInput(String key, Object value) {
        return simpleInput(MapBuilder.<String, Object>newMapBuilder().put(key, value));
    }

    public static SimpleInput.Builder simpleInput(MapBuilder<String, Object> data) {
        return simpleInput(data.map());
    }

    public static SimpleInput.Builder simpleInput(Map<String, Object> data) {
        return SimpleInput.builder(new Payload.Simple(data));
    }

    public static HttpInput.Builder httpInput(HttpRequestTemplate.Builder request) {
        return httpInput(request.build());
    }

    public static HttpInput.Builder httpInput(HttpRequestTemplate request) {
        return HttpInput.builder(request);
    }

    public static ChainInput.Builder chainInput() {
        return ChainInput.builder();
    }
}
