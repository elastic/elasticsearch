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
package org.elasticsearch.index.search.child;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.search.NoopCollector;

import java.io.IOException;

class BitSetCollector extends NoopCollector {

    final FixedBitSet result;
    int docBase;

    BitSetCollector(int topLevelMaxDoc) {
        this.result = new FixedBitSet(topLevelMaxDoc);
    }

    @Override
    public void collect(int doc) throws IOException {
        result.set(docBase + doc);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        docBase = context.docBase;
    }

    FixedBitSet getResult() {
        return result;
    }

}
