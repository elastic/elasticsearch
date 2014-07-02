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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedDocValues;

import java.util.Set;

/**
 * Specialization of {@link AtomicFieldData} for parent/child mappings.
 */
public interface AtomicParentChildFieldData extends AtomicFieldData {

    /**
     * Return the set of types there is a mapping for.
     */
    Set<String> types();

    /**
     * Return the mapping for the given type. The returned
     * {@link SortedDocValues} will map doc IDs to the identifier of their
     * parent.
     */
    SortedDocValues getOrdinalsValues(String type);

}
