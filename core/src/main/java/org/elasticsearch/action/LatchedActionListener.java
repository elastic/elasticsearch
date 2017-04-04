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

package org.elasticsearch.action;

import java.util.concurrent.CountDownLatch;

/**
 * An action listener that allows passing in a {@link CountDownLatch} that
 * will be counted down after onResponse or onFailure is called
 */
public class LatchedActionListener<T> implements ActionListener<T> {

    private final ActionListener<T> delegate;
    private final CountDownLatch latch;

    public LatchedActionListener(ActionListener<T> delegate, CountDownLatch latch) {
        this.delegate = delegate;
        this.latch = latch;
    }

    @Override
    public void onResponse(T t) {
        try {
            delegate.onResponse(t);
        } finally {
            latch.countDown();
        }
    }

    @Override
    public void onFailure(Exception e) {
        try {
            delegate.onFailure(e);
        } finally {
            latch.countDown();
        }
    }
}
