/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import com.fasterxml.jackson.annotation.JacksonAnnotation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class PluginDescriptor {
    private String id;
    private String url;
    private String proxy;

    @JsonCreator
    public PluginDescriptor(@JsonProperty("id") String id, @JsonProperty("url") String url, @JsonProperty("proxy") String proxy) {
        this.id = id;
        this.url = url;
        this.proxy = proxy;
    }

    public PluginDescriptor(String id, String url) {
        this(id, url, null);
    }

    public PluginDescriptor(String id) {
        this(id, null, null);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getProxy() {
        return proxy;
    }

    public void setProxy(String proxy) {
        this.proxy = proxy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PluginDescriptor that = (PluginDescriptor) o;
        return id.equals(that.id) && Objects.equals(url, that.url) && Objects.equals(proxy, that.proxy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, url, proxy);
    }

    @Override
    public String toString() {
        return String.format("PluginDescriptor{id='%s', url='%s', proxy='%s'}", id, url, proxy);
    }
}
