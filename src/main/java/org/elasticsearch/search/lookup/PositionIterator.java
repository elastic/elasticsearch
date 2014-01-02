/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchException;

import java.io.IOException;
import java.util.Iterator;

public class PositionIterator implements Iterator<TermPosition> {
    
    private static final DocsAndPositionsEnum EMPTY = new EmptyDocsAndPosEnum();
    
    private boolean resetted = false;

    protected ScriptTerm scriptTerm;

    protected int freq = -1;

    // current position of iterator
    private int currentPos;

    protected final TermPosition termPosition = new TermPosition();

    private DocsAndPositionsEnum docsAndPos;

    public PositionIterator(ScriptTerm termInfo) {
        this.scriptTerm = termInfo;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot remove anything from TermPositions iterator.");
    }

    @Override
    public boolean hasNext() {
        return currentPos < freq;
    }


    @Override
    public TermPosition next() {
        try {
            termPosition.position = docsAndPos.nextPosition();
            termPosition.startOffset = docsAndPos.startOffset();
            termPosition.endOffset = docsAndPos.endOffset();
            termPosition.payload = docsAndPos.getPayload();
        } catch (IOException ex) {
            throw new ElasticSearchException("can not advance iterator", ex);
        }
        currentPos++;
        return termPosition;
    }

    public void nextDoc() throws IOException {
        resetted = false;
        currentPos = 0;
        freq = scriptTerm.tf();
        if (scriptTerm.docsEnum instanceof DocsAndPositionsEnum) {
            docsAndPos = (DocsAndPositionsEnum) scriptTerm.docsEnum;
        } else {
            docsAndPos = EMPTY;
        }
    }

    public Iterator<TermPosition> reset() {
        if (resetted) {
            throw new ElasticSearchException(
                    "Cannot iterate twice! If you want to iterate more that once, add _CACHE explicitely.");
        }
        resetted = true;
        return this;
    }

    // we use this to make sure we can also iterate if there are no positions
    private static final class EmptyDocsAndPosEnum extends DocsAndPositionsEnum {

        @Override
        public int nextPosition() throws IOException {
            return -1;
        }

        @Override
        public int startOffset() throws IOException {
            return -1;
        }

        @Override
        public int endOffset() throws IOException {
            return -1;
        }

        @Override
        public BytesRef getPayload() throws IOException {
            return null;
        }

        @Override
        public int freq() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docID() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int nextDoc() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int advance(int target) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            throw new UnsupportedOperationException();
        }
    }
}
