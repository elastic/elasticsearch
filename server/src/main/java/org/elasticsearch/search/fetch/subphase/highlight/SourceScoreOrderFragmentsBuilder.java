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
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.ScoreOrderFragmentsBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.List;

public class SourceScoreOrderFragmentsBuilder extends ScoreOrderFragmentsBuilder {

    private final MappedFieldType fieldType;

    private final SearchContext searchContext;

    public SourceScoreOrderFragmentsBuilder(MappedFieldType fieldType,
                                            SearchContext searchContext,
                                            String[] preTags,
                                            String[] postTags,
                                            BoundaryScanner boundaryScanner) {
        super(preTags, postTags, boundaryScanner);
        this.fieldType = fieldType;
        this.searchContext = searchContext;
    }

    @Override
    protected Field[] getFields(IndexReader reader, int docId, String fieldName) throws IOException {
        // we know its low level reader, and matching docId, since that's how we call the highlighter with
        SourceLookup sourceLookup = searchContext.lookup().source();
        sourceLookup.setSegmentAndDocument((LeafReaderContext) reader.getContext(), docId);

        List<Object> values = sourceLookup.extractRawValues(fieldType.name());
        Field[] fields = new Field[values.size()];
        for (int i = 0; i < values.size(); i++) {
            fields[i] = new Field(fieldType.name(), values.get(i).toString(), TextField.TYPE_NOT_STORED);
        }
        return fields;
    }

    @Override
    protected String makeFragment( StringBuilder buffer, int[] index, Field[] values, WeightedFragInfo fragInfo,
            String[] preTags, String[] postTags, Encoder encoder ){
        WeightedFragInfo weightedFragInfo = FragmentBuilderHelper.fixWeightedFragInfo(fieldType, values, fragInfo);
        return super.makeFragment(buffer, index, values, weightedFragInfo, preTags, postTags, encoder);
    }
}
