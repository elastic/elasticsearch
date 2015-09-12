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

/**
 * A container for a {@link MappedFieldType} which can be updated and is reference counted.
 */
public class MappedFieldTypeReference {
    private MappedFieldType fieldType; // the current field type this reference points to
    private int numAssociatedMappers;

    public MappedFieldTypeReference(MappedFieldType fieldType) {
        fieldType.freeze(); // ensure frozen
        this.fieldType = fieldType;
        this.numAssociatedMappers = 1;
    }

    public MappedFieldType get() {
        return fieldType;
    }

    public void set(MappedFieldType fieldType) {
        fieldType.freeze(); // ensure frozen
        this.fieldType = fieldType;
    }

    public int getNumAssociatedMappers() {
        return numAssociatedMappers;
    }

    public void incrementAssociatedMappers() {
        ++numAssociatedMappers;
    }
}
