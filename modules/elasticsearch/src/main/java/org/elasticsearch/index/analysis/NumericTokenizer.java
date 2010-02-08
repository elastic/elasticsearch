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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.Tokenizer;

import java.io.IOException;
import java.io.Reader;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class NumericTokenizer extends Tokenizer {

    private final NumericTokenStream numericTokenStream;

    protected NumericTokenizer(Reader reader, NumericTokenStream numericTokenStream) throws IOException {
        super(numericTokenStream);
        this.numericTokenStream = numericTokenStream;
        reset(reader);
    }

    protected NumericTokenizer(Reader reader, NumericTokenStream numericTokenStream, char[] buffer) throws IOException {
        super(numericTokenStream);
        this.numericTokenStream = numericTokenStream;
        reset(reader, buffer);
    }

    @Override public void reset(Reader input) throws IOException {
        char[] buffer = new char[32];
        reset(input, buffer);
    }

    public void reset(Reader input, char[] buffer) throws IOException {
        super.reset(input);
        int len = super.input.read(buffer);
        String value = new String(buffer, 0, len);
        setValue(numericTokenStream, value);
        numericTokenStream.reset();
    }

    @Override public boolean incrementToken() throws IOException {
        return numericTokenStream.incrementToken();
    }

    protected abstract void setValue(NumericTokenStream tokenStream, String value);
}
