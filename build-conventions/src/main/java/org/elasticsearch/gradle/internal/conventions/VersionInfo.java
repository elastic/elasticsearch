/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions;

import groovy.lang.GroovyObject;
import groovy.lang.MetaClass;

import java.util.Properties;

public class VersionInfo {
    private final String elasticsearch;
    private final String lucene;
    private final String bundledJdkVendor;
    private final String bundledJdkVersion;
    private Properties properties;
    private MetaClass metaClass;

    public VersionInfo(Properties properties) {
        elasticsearch = properties.getProperty("elasticsearch");
        lucene = properties.getProperty("lucene");
        bundledJdkVendor = properties.getProperty("bundled_jdk_vendor");
        bundledJdkVersion = properties.getProperty("bundled_jdk");
        this.properties = properties;
    }

    public String getElasticsearch() {
        return elasticsearch;
    }

    public String getLucene() {
        return lucene;
    }

    public String getBundledJdkVendor() {
        return bundledJdkVendor;
    }

    public String getBundledJdkVersion() {
        return bundledJdkVersion;
    }
    public boolean isElasticsearchSnapshot() {
        return getElasticsearch().endsWith("-SNAPSHOT");
    }

    public Object get(Object key) {
        return properties.getProperty(key.toString());
    }

    public Object propertyMissing(String name) {
        return getProperty(name);
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public Properties getProperties() {
        return properties;
    }
}
