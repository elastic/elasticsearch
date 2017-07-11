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
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.elasticsearch.index.mapper.FieldMapper;

/**
 * Direct Subclass of Lucene's org.apache.lucene.search.vectorhighlight.SimpleFragmentsBuilder 
 * that corrects offsets for broken analysis chains. 
 */
public class SimpleFragmentsBuilder extends org.apache.lucene.search.vectorhighlight.SimpleFragmentsBuilder {
    protected final FieldMapper mapper;

    public SimpleFragmentsBuilder(FieldMapper mapper,
                                        String[] preTags, String[] postTags, BoundaryScanner boundaryScanner) {
        super(preTags, postTags, boundaryScanner);
        this.mapper = mapper;
    }
    
    @Override
    protected String makeFragment( StringBuilder buffer, int[] index, Field[] values, WeightedFragInfo fragInfo,
            String[] preTags, String[] postTags, Encoder encoder ){
        return super.makeFragment(buffer, index, values, FragmentBuilderHelper.fixWeightedFragInfo(mapper, values, fragInfo),
                preTags, postTags, encoder);
   }
}
