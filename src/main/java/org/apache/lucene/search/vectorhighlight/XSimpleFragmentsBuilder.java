package org.apache.lucene.search.vectorhighlight;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;

import java.util.List;

/**
 * A simple implementation of FragmentsBuilder.
 */
public class XSimpleFragmentsBuilder extends AbstractFragmentsBuilder {

    /**
     * a constructor.
     */
    public XSimpleFragmentsBuilder() {
        super();
    }

    /**
     * a constructor.
     *
     * @param preTags  array of pre-tags for markup terms.
     * @param postTags array of post-tags for markup terms.
     */
    public XSimpleFragmentsBuilder(String[] preTags, String[] postTags) {
        super(preTags, postTags);
    }

    public XSimpleFragmentsBuilder(BoundaryScanner bs) {
        super(bs);
    }

    public XSimpleFragmentsBuilder(String[] preTags, String[] postTags, BoundaryScanner bs) {
        super(preTags, postTags, bs);
    }

    /**
     * do nothing. return the source list.
     */
    @Override
    public List<WeightedFragInfo> getWeightedFragInfoList(List<WeightedFragInfo> src) {
        return src;
    }
}
