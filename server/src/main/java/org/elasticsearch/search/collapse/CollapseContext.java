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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Context used for field collapsing
 */
public class CollapseContext {
    private final MappedFieldType fieldType;
    private final List<InnerHitBuilder> innerHits;

    public CollapseContext(MappedFieldType fieldType, InnerHitBuilder innerHit) {
        this.fieldType = fieldType;
        this.innerHits = Collections.singletonList(innerHit);
    }

    public CollapseContext(MappedFieldType fieldType, List<InnerHitBuilder> innerHits) {
        this.fieldType = fieldType;
        this.innerHits = innerHits;
    }

    /** The field type used for collapsing **/
    public MappedFieldType getFieldType() {
        return fieldType;
    }

    /** The inner hit options to expand the collapsed results **/
    public List<InnerHitBuilder> getInnerHit() {
        return innerHits;
    }

    public CollapsingTopDocsCollector<?> createTopDocs(Sort sort, int topN, boolean trackMaxScore) {
        if (fieldType instanceof KeywordFieldMapper.KeywordFieldType) {
            return CollapsingTopDocsCollector.createKeyword(fieldType.name(), sort, topN, trackMaxScore);
        } else if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
            return CollapsingTopDocsCollector.createNumeric(fieldType.name(), sort, topN, trackMaxScore);
        } else {
            throw new IllegalStateException("unknown type for collapse field " + fieldType.name() +
                ", only keywords and numbers are accepted");
        }
    }
}
