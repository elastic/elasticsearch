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

import java.io.IOException;
import java.util.Iterator;

public abstract class PositionIterator implements Iterator<TermPosition> {

    ScriptTerm scriptTerm;

    int freq = -1;

    // current position of iterator
    protected int curIteratorPos;

    final protected TermPosition termPosition = new TermPosition();

    protected DocsAndPositionsEnum docsAndPos;

    public PositionIterator(ScriptTerm termInfo) {
        this.scriptTerm = termInfo;
    }

    protected abstract void init() throws IOException;

    protected abstract void initDocsAndPos() throws IOException;

    public abstract Iterator<TermPosition> reset();

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot remove anything from TermPositions iterator.");
    }

    @Override
    public boolean hasNext() {
        return curIteratorPos < freq;
    }

}
