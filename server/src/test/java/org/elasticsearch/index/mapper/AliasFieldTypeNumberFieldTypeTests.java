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

public class AliasFieldTypeNumberFieldTypeTests extends NumberFieldTypeTests {

    private static final String ALIASFIELD = "alias2";
    private final String PATHTO = NUMBERFIELD;

    @Override
    protected AliasFieldMapper.AliasFieldType createDefaultFieldType() {
        return new AliasFieldMapper.AliasFieldType(null,
            null,
            PATHTO,
            new NumberFieldMapper.NumberFieldType(this.type));
    }

    @Override
    MappedFieldType createNamedDefaultFieldType() {
        final AliasFieldMapper.AliasFieldType fieldType = this.createDefaultFieldType();
        fieldType.setName(ALIASFIELD);
        fieldType.aliasTarget().setName(this.fieldTypeName());
        return fieldType;
    }

    @Override
    String fieldInMessage() {
        return AliasFieldMapper.AliasFieldType.nameInMessage(ALIASFIELD, PATHTO);
    }
}
