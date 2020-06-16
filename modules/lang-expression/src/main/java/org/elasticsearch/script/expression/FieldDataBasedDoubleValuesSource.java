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

package org.elasticsearch.script.expression;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.util.Objects;

abstract class FieldDataBasedDoubleValuesSource extends DoubleValuesSource {

    FieldDataBasedDoubleValuesSource(IndexFieldData<?> fieldData) {
        this.fieldData = Objects.requireNonNull(fieldData);
    }

    protected final IndexFieldData<?> fieldData;

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) {
        return this;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, fieldData.getFieldName());
    }

}
