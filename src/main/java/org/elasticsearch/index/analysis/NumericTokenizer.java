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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.Tokenizer;

import java.io.IOException;
import java.io.Reader;

/**
 *
 */
public abstract class NumericTokenizer extends Tokenizer {

    private final NumericTokenStream numericTokenStream;
    private final char[] buffer;
    protected final Object extra;

    protected NumericTokenizer(Reader reader, NumericTokenStream numericTokenStream, Object extra) throws IOException {
        this(reader, numericTokenStream, new char[32], extra);
    }

    protected NumericTokenizer(Reader reader, NumericTokenStream numericTokenStream, char[] buffer, Object extra) throws IOException {
        super(reader);
        this.numericTokenStream = numericTokenStream;
        this.extra = extra;
        this.buffer = buffer;
    }

    @Override
    public void reset() throws IOException {
        reset(buffer);
    }

    public void reset(char[] buffer) throws IOException {
        int len = input.read(buffer);
        String value = new String(buffer, 0, len);
        setValue(numericTokenStream, value);
        numericTokenStream.reset();
    }

    @Override
    public final boolean incrementToken() throws IOException {
        return numericTokenStream.incrementToken();
    }

    protected abstract void setValue(NumericTokenStream tokenStream, String value);
}
