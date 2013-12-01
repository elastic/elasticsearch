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

package org.elasticsearch.search.lookup.termstatistics;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.elasticsearch.ElasticSearchException;

import java.io.IOException;
import java.util.Iterator;

public class UnrecordedPositionIterator extends PositionIterator {

    public UnrecordedPositionIterator(ScriptTerm termInfo) {
        super(termInfo);
    }

    @Override
    public TermPosition next() {
        if (!hasNext()) {
            return null;
        }
        if (docsAndPos != null) {
            try {
                initTermPosition(docsAndPos);
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
        curIteratorPos++;
        return termPosition;
    }

    protected void initTermPosition(DocsAndPositionsEnum docsAndPos) throws IOException {

        int nextPos = docsAndPos.nextPosition();
        if (shouldRetrievePositions()) {
            termPosition.position = nextPos;
        } 
        if (shouldRetrieveOffsets()) {
            termPosition.startOffset = docsAndPos.startOffset();
            termPosition.endOffset = docsAndPos.endOffset();
        }
        if (shouldRetrievePayloads()) {
            termPosition.payload = docsAndPos.getPayload();
        }
    }

    protected boolean shouldRetrievePositions() {
        return scriptTerm.shouldRetrievePositions();
    }

    protected boolean shouldRetrieveOffsets() {
        return scriptTerm.shouldRetrieveOffsets();
    }

    protected boolean shouldRetrievePayloads() {
        return scriptTerm.shouldRetrievePayloads();
    }

    @Override
    void init() throws IOException {
        curIteratorPos = 0;
    }

    @Override
    public Iterator<TermPosition> reset() {
        if (curIteratorPos != 0) {
            throw new ElasticSearchException(
                    "No more positions to return! If you want to iterate more that once, remove _DO_NOT_RECORD flag or call record() explicitely.");
        }
        return this;
    }

    @Override
    void initDocsAndPos() throws IOException {
        if (scriptTerm.docsEnum instanceof DocsAndPositionsEnum) {
            docsAndPos = (DocsAndPositionsEnum) scriptTerm.docsEnum;
        } else {
            docsAndPos = null;
        }
    }
}
