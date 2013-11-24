/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.support.numeric.ValueParser;

/**
 *
 */
public class ValuesSourceConfig<VS extends ValuesSource> {

    final Class<VS> valueSourceType;
    FieldContext fieldContext;
    SearchScript script;
    ValueFormatter formatter;
    ValueParser parser;
    ScriptValueType scriptValueType;
    boolean unmapped = false;
    boolean needsHashes = false;
    boolean ensureUnique = false;
    boolean ensureSorted = false;

    public ValuesSourceConfig(Class<VS> valueSourceType) {
        this.valueSourceType = valueSourceType;
    }

    public Class<VS> valueSourceType() {
        return valueSourceType;
    }

    public FieldContext fieldContext() {
        return fieldContext;
    }

    public boolean unmapped() {
        return unmapped;
    }

    public boolean valid() {
        return fieldContext != null || script != null || unmapped;
    }

    public ValuesSourceConfig<VS> fieldContext(FieldContext fieldContext) {
        this.fieldContext = fieldContext;
        return this;
    }

    public ValuesSourceConfig<VS> script(SearchScript script) {
        this.script = script;
        return this;
    }

    public ValuesSourceConfig<VS> formatter(ValueFormatter formatter) {
        this.formatter = formatter;
        return this;
    }

    public ValueFormatter formatter() {
        return formatter;
    }

    public ValuesSourceConfig<VS> parser(ValueParser parser) {
        this.parser = parser;
        return this;
    }

    public ValueParser parser() {
        return parser;
    }

    public ValuesSourceConfig<VS> scriptValueType(ScriptValueType scriptValueType) {
        this.scriptValueType = scriptValueType;
        return this;
    }

    public ScriptValueType scriptValueType() {
        return scriptValueType;
    }

    public ValuesSourceConfig<VS> unmapped(boolean unmapped) {
        this.unmapped = unmapped;
        return this;
    }

    public ValuesSourceConfig<VS> needsHashes(boolean needsHashes) {
        this.needsHashes = needsHashes;
        return this;
    }

    public ValuesSourceConfig<VS> ensureUnique(boolean unique) {
        this.ensureUnique = unique;
        return this;
    }

    public ValuesSourceConfig<VS> ensureSorted(boolean sorted) {
        this.ensureSorted = sorted;
        return this;
    }
}
