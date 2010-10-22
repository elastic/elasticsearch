/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.common.lucene.docset;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class NotDocSet extends GetDocSet {

    private final DocSet set;

    public NotDocSet(DocSet set, int max) {
        super(max);
        this.set = set;
    }

    @Override public boolean isCacheable() {
        // if it is cached, create a new doc set for it so it will be fast for advance in iterator
        return false;
    }

    @Override public boolean get(int doc) throws IOException {
        return !set.get(doc);
    }
}
