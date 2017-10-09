/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle

/**
 * Accessor for shared dependency versions used by elasticsearch, namely the elasticsearch and lucene versions.
 */
class VersionProperties {
    static final String elasticsearch
    static final String lucene
    static final Map<String, String> versions = new HashMap<>()
    static {
        Properties props = new Properties()
        InputStream propsStream = VersionProperties.class.getResourceAsStream('/version.properties')
        if (propsStream == null) {
            throw new RuntimeException('/version.properties resource missing')
        }
        props.load(propsStream)
        elasticsearch = props.getProperty('elasticsearch')
        lucene = props.getProperty('lucene')
        for (String property : props.stringPropertyNames()) {
            versions.put(property, props.getProperty(property))
        }
    }
}
