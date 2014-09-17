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
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreaker;

import java.io.IOException;

/**
 * Estimator that does nothing except for adjust the breaker after the field
 * data has been loaded. Useful for field data implementations that do not yet
 * have pre-loading estimations.
 */
public class NonEstimatingEstimator implements AbstractIndexFieldData.PerValueEstimator {

    private final CircuitBreaker breaker;

    NonEstimatingEstimator(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    @Override
    public long bytesPerValue(BytesRef term) {
        return 0;
    }

    @Override
    public TermsEnum beforeLoad(Terms terms) throws IOException {
        return null;
    }

    @Override
    public void afterLoad(@Nullable TermsEnum termsEnum, long actualUsed) {
        breaker.addWithoutBreaking(actualUsed);
    }
}
