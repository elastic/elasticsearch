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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeSource;
import org.elasticsearch.common.io.Streams;

import java.io.IOException;
import java.util.Iterator;

/**
 *
 */
public abstract class NumericTokenizer extends Tokenizer {

    /** Make this tokenizer get attributes from the delegate token stream. */
    private static final AttributeFactory delegatingAttributeFactory(final AttributeSource source) {
        return new AttributeFactory() {
            @Override
            public AttributeImpl createAttributeInstance(Class<? extends Attribute> attClass) {
                return (AttributeImpl) source.addAttribute(attClass);
            }
        };
    }

    private final NumericTokenStream numericTokenStream;
    private final char[] buffer;
    protected final Object extra;
    private boolean started;

    protected NumericTokenizer(NumericTokenStream numericTokenStream, char[] buffer, Object extra) throws IOException {
        super(delegatingAttributeFactory(numericTokenStream));
        this.numericTokenStream = numericTokenStream;
        // Add attributes from the numeric token stream, this works fine because the attribute factory delegates to numericTokenStream
        for (Iterator<Class<? extends Attribute>> it = numericTokenStream.getAttributeClassesIterator(); it.hasNext();) {
            addAttribute(it.next());
        }
        this.extra = extra;
        this.buffer = buffer;
        started = true;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        started = false;
    }

    @Override
    public final boolean incrementToken() throws IOException {
        if (!started) {
            // reset() must be idempotent, this is why we read data in incrementToken
            final int len = Streams.readFully(input, buffer);
            if (len == buffer.length && input.read() != -1) {
                throw new IOException("Cannot read numeric data larger than " + buffer.length + " chars");
            }
            setValue(numericTokenStream, new String(buffer, 0, len));
            numericTokenStream.reset();
            started = true;
        }
        return numericTokenStream.incrementToken();
    }

    @Override
    public void end() throws IOException {
        super.end();
        numericTokenStream.end();
    }

    @Override
    public void close() throws IOException {
        super.close();
        numericTokenStream.close();
    }

    protected abstract void setValue(NumericTokenStream tokenStream, String value);
}
