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

package org.elasticsearch.search.lookup;

import java.util.List;


/**
 * Script level doc values, the assumption is that any implementation will implement a <code>getValue</code>
 * and a <code>getValues</code> that return the relevant type that then can be used in scripts.
 */
public interface ScriptDocValues<T> extends List<T> {

    /**
     * Set the current doc ID.
     */
    void setNextDocId(int docId);

    /**
     * Return a copy of the list of the values for the current document.
     */
    List<T> getValues();
}
