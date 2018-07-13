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
package org.elasticsearch.search.collapse;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.CollapsingTopDocsCollector;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.InnerHitBuilder;

import java.util.Collections;
import java.util.List;

/**
 * Context used for field collapsing
 */
public class CollapseContext {
    private final List<InnerHitBuilder> innerHits;
    private final String[] fieldNames;
    private final MappedFieldType[] fieldTypes;

    public CollapseContext(String[] fieldNames, MappedFieldType[] fieldTypes, InnerHitBuilder innerHit) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.innerHits = Collections.singletonList(innerHit);
    }

    public CollapseContext(String[] fieldNames, MappedFieldType[] fieldTypes, List<InnerHitBuilder> innerHits) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.innerHits = innerHits;
    }

    /** The field types used for collapsing **/
    public MappedFieldType[] getFieldTypes() {
        return fieldTypes;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    /** The inner hit options to expand the collapsed results **/
    public List<InnerHitBuilder> getInnerHit() {
        return innerHits;
    }

    public CollapsingTopDocsCollector<?> createTopDocs(Sort sort, int topN, boolean trackMaxScore) {
        //TODO: limit the number of fields to collapse on
        byte keywordsCount = 0;
        byte numericCount = 0;
        for (int i = 0; i<fieldTypes.length; i++) {
            if (fieldTypes[i] instanceof KeywordFieldMapper.KeywordFieldType) {
                keywordsCount++;
            } else if (fieldTypes[i] instanceof NumberFieldMapper.NumberFieldType) {
               numericCount++;
            } else {
                throw new IllegalStateException("Unknown type for collapse field ["  + fieldNames[i] +
                    "], only keywords and numbers are accepted");
            }
        }
        if (keywordsCount == fieldTypes.length){
            return CollapsingTopDocsCollector.createMultipleKeyword(fieldNames, sort, topN, trackMaxScore);
        } else if (numericCount == fieldTypes.length) {
            return CollapsingTopDocsCollector.createMultipleNumeric(fieldNames, sort, topN, trackMaxScore);
        } else {
            throw new IllegalStateException("All fields for multiple collapse should be of the same type, either [keyword] or [numeric]!");
        }
    }
}
