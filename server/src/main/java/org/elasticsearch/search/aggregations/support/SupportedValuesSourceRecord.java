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

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;

import java.time.ZoneId;
import java.time.ZoneOffset;

public class SupportedValuesSourceRecord {
    private final ValuesSourceMatcher matcher;
    private final ValuesSourceFamily family;
    private final DocValueFormat docValueFormat;

    public ValuesSourceFamily getFamily() {
        return family;
    }

    public DocValueFormat resolveFormat(String format, ZoneId tz) {
        //TODO: Make this plugable
        if (docValueFormat == null) {
            return DocValueFormat.RAW; // we can't figure it out
        }
        DocValueFormat valueFormat = docValueFormat;
        if (valueFormat instanceof DocValueFormat.Decimal && format != null) {
            valueFormat = new DocValueFormat.Decimal(format);
        }
        if (valueFormat instanceof DocValueFormat.DateTime && format != null) {
            valueFormat = new DocValueFormat.DateTime(DateFormatter.forPattern(format), tz != null ? tz : ZoneOffset.UTC,
                DateFieldMapper.Resolution.MILLISECONDS);
        }
        return valueFormat;
    }

    public boolean matches(QueryShardContext context, ValueType valueType, String field, Script script) {
        return matcher.matches(context, valueType, field, script);
    }

    public SupportedValuesSourceRecord(ValuesSourceMatcher matcher, ValuesSourceFamily family, DocValueFormat docValueFormat) {
        this.matcher = matcher;
        this.family = family;
        this.docValueFormat = docValueFormat;
    }
}
