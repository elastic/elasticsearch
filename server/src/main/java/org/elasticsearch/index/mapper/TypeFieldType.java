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

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.ConstantIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

@Deprecated
public final class TypeFieldType extends ConstantFieldType {

    public static final String NAME = "_type";

    public static final TypeFieldType INSTANCE = new TypeFieldType();

    private TypeFieldType() {
        freeze();
    }

    @Override
    public MappedFieldType clone() {
        return this;
    }

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
        return new ConstantIndexFieldData.Builder(s -> MapperService.SINGLE_MAPPING_NAME);
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected boolean matches(String pattern, QueryShardContext context) {
        return pattern.equals(MapperService.SINGLE_MAPPING_NAME);
    }
}
