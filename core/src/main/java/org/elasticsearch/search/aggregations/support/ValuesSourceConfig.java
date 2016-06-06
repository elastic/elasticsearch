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
package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

/**
 *
 */
public class ValuesSourceConfig<VS extends ValuesSource> {

    private final ValuesSourceType valueSourceType;
    private FieldContext fieldContext;
    private SearchScript script;
    private ValueType scriptValueType;
    private boolean unmapped = false;
    private String formatPattern;
    private DocValueFormat format = DocValueFormat.RAW;
    private Object missing;
    private DateTimeZone timeZone;

    public ValuesSourceConfig(ValuesSourceType valueSourceType) {
        this.valueSourceType = valueSourceType;
    }

    public ValuesSourceType valueSourceType() {
        return valueSourceType;
    }

    public FieldContext fieldContext() {
        return fieldContext;
    }

    public SearchScript script() {
        return script;
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

    public ValuesSourceConfig<VS> scriptValueType(ValueType scriptValueType) {
        this.scriptValueType = scriptValueType;
        return this;
    }

    public ValueType scriptValueType() {
        return this.scriptValueType;
    }

    public ValuesSourceConfig<VS> unmapped(boolean unmapped) {
        this.unmapped = unmapped;
        return this;
    }

    public ValuesSourceConfig<VS> format(final DocValueFormat format) {
        this.format = format;
        return this;
    }

    public ValuesSourceConfig<VS> missing(final Object missing) {
        this.missing = missing;
        return this;
    }

    public Object missing() {
        return this.missing;
    }

    public ValuesSourceConfig<VS> timezone(final DateTimeZone timeZone) {
        this.timeZone= timeZone;
        return this;
    }

    public DateTimeZone timezone() {
        return this.timeZone;
    }

    public DocValueFormat format() {
        return format;
    }
}
