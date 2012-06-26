/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.selector;

import org.apache.lucene.document.FieldSelectorResult;
import org.elasticsearch.common.lucene.document.ResetFieldSelector;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

/**
 * An optimized field selector that loads just the uid.
 */
public class UidFieldSelector implements ResetFieldSelector {

    public static final UidFieldSelector INSTANCE = new UidFieldSelector();

    private UidFieldSelector() {

    }

    @Override
    public FieldSelectorResult accept(String fieldName) {
        if (UidFieldMapper.NAME.equals(fieldName)) {
            return FieldSelectorResult.LOAD_AND_BREAK;
        }
        return FieldSelectorResult.NO_LOAD;
    }

    @Override
    public void reset() {
    }

    @Override
    public String toString() {
        return "uid";
    }
}