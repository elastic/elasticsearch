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

package org.elasticsearch.common.lucene.all;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

public class AllField extends Field {
    private final float boost;

    public AllField(String name, String value, float boost, FieldType fieldType) {
        super(name, value, fieldType);
        this.boost = boost;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream previous) {
        TokenStream ts = analyzer.tokenStream(name(), stringValue());
        if (boost != 1.0f && fieldType().indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
            // TODO: we should be able to reuse "previous" if its instanceof AllTokenStream?
            // but we need to be careful this optimization is safe (and tested)...

            // AllTokenStream maps boost to 4-byte payloads, so we only need to use it any field had non-default (!= 1.0f) boost and if
            // positions are indexed:
            return new AllTokenStream(ts, boost);
        }
        return ts;
    }
}
