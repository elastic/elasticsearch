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

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;

public enum BuiltInValuesSourceMatchers implements ValuesSourceMatcher {
    ANY {
        @Override
        public boolean matches(QueryShardContext context, ValueType valueType, String field, Script script) {
            return true;
        }
    },
    UNMAPPED {
        @Override
        public boolean matches(QueryShardContext context, ValueType valueType, String field, Script script) {
            return field != null && context.fieldMapper(field) == null;
        }
    },
    DATE {
        @Override
        public boolean matches(QueryShardContext context, ValueType valueType, String field, Script script) {
            if (valueType == ValueType.DATE) {
                return true;
            }
            return context.fieldMapper(field) instanceof DateFieldMapper.DateFieldType;
        }
    },
    /**
     * Built in numbers that aren't dates
     */
    RAW_NUMBER{
        @Override
        public boolean matches(QueryShardContext context, ValueType valueType, String field, Script script) {
            if (valueType != null && valueType.valuesSourceFamily == ValuesSourceType.NUMERIC && valueType != ValueType.DATE) {
               return true;
            }
            return getIndexFieldData(context, field) instanceof IndexNumericFieldData;
        }
    },
    IP {
        @Override
        public boolean matches(QueryShardContext context, ValueType valueType, String field, Script script) {
            if (valueType == ValueType.IP) {
                return true;
            }
            return context.fieldMapper(field) instanceof IpFieldMapper.IpFieldType;
        }
    },
    STRING {
        @Override
        public boolean matches(QueryShardContext context, ValueType valueType, String field, Script script) {
            if (valueType == ValueType.STRING) {
                return true;
            }
            return context.fieldMapper(field) instanceof KeywordFieldMapper.KeywordFieldType;
        }
    }

    ;

    @Override
    public abstract boolean matches(QueryShardContext context, ValueType valueType, String field, Script script);

    private static IndexFieldData getIndexFieldData(QueryShardContext context, String field) {
        if (field != null) {
            MappedFieldType fieldType = context.fieldMapper(field);
            if (fieldType != null) {
                return context.getForField(fieldType);
            }
        }
        return null;
    }
}
