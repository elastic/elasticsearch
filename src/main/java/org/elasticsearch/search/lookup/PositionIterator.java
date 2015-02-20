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

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.PostingsEnum;
import org.elasticsearch.ElasticsearchException;

import java.io.IOException;
import java.util.Iterator;

public class PositionIterator implements Iterator<TermPosition> {
    
    private boolean resetted = false;

    protected IndexFieldTerm indexFieldTerm;

    protected int freq = -1;

    // current position of iterator
    private int currentPos;

    protected final TermPosition termPosition = new TermPosition();

    private PostingsEnum postings;

    public PositionIterator(IndexFieldTerm indexFieldTerm) {
        this.indexFieldTerm = indexFieldTerm;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot remove anything from TermPosition iterator.");
    }

    @Override
    public boolean hasNext() {
        return currentPos < freq;
    }


    @Override
    public TermPosition next() {
        try {
            termPosition.position = postings.nextPosition();
            termPosition.startOffset = postings.startOffset();
            termPosition.endOffset = postings.endOffset();
            termPosition.payload = postings.getPayload();
        } catch (IOException ex) {
            throw new ElasticsearchException("can not advance iterator", ex);
        }
        currentPos++;
        return termPosition;
    }

    public void nextDoc() throws IOException {
        resetted = false;
        currentPos = 0;
        freq = indexFieldTerm.tf();
        postings = indexFieldTerm.postings;
    }

    public Iterator<TermPosition> reset() {
        if (resetted) {
            throw new ElasticsearchException(
                    "Cannot iterate twice! If you want to iterate more that once, add _CACHE explicitly.");
        }
        resetted = true;
        return this;
    }
}
