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
package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.fielddata.plain.AbstractIndexOrdinalsFieldData;

import java.io.IOException;

/**
 * {@link TermsEnum} that takes a CircuitBreaker, increasing the breaker
 * every time {@code .next(...)} is called. Proxies all methods to the original
 * TermsEnum otherwise.
 */
public final class RamAccountingTermsEnum extends FilteredTermsEnum {

    // Flush every 5mb
    private static final long FLUSH_BUFFER_SIZE = 1024 * 1024 * 5;

    private final CircuitBreaker breaker;
    private final TermsEnum termsEnum;
    private final AbstractIndexOrdinalsFieldData.PerValueEstimator estimator;
    private final String fieldName;
    private long totalBytes;
    private long flushBuffer;


    public RamAccountingTermsEnum(TermsEnum termsEnum, CircuitBreaker breaker,
                                  AbstractIndexOrdinalsFieldData.PerValueEstimator estimator,
                                  String fieldName) {
        super(termsEnum);
        this.breaker = breaker;
        this.termsEnum = termsEnum;
        this.estimator = estimator;
        this.fieldName = fieldName;
        this.totalBytes = 0;
        this.flushBuffer = 0;
    }

    /**
     * Always accept the term.
     */
    @Override
    protected AcceptStatus accept(BytesRef term) throws IOException {
        return AcceptStatus.YES;
    }

    /**
     * Flush the {@code flushBuffer} to the breaker, incrementing the total
     * bytes and resetting the buffer.
     */
    public void flush() {
        breaker.addEstimateBytesAndMaybeBreak(this.flushBuffer, this.fieldName);
        this.totalBytes += this.flushBuffer;
        this.flushBuffer = 0;
    }

    /**
     * Proxy to the original next() call, but estimates the overhead of
     * loading the next term.
     */
    @Override
    public BytesRef next() throws IOException {
        BytesRef term = termsEnum.next();
        if (term == null && this.flushBuffer != 0) {
            // We have reached the end of the termsEnum, flush the buffer
            flush();
        } else {
            this.flushBuffer += estimator.bytesPerValue(term);
            if (this.flushBuffer >= FLUSH_BUFFER_SIZE) {
                flush();
            }
        }
        return term;
    }

    /**
     * @return the total number of bytes that have been aggregated
     */
    public long getTotalBytes() {
        return this.totalBytes;
    }
}
