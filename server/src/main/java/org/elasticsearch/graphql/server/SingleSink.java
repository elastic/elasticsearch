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

package org.elasticsearch.graphql.server;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class SingleSink<T> implements Publisher<T> {
    private Subscriber<? super T> subscriber = null;

    public void subscribe(Subscriber<? super T> s) {
        if (subscriber != null) {
            System.out.println("SingleSink allows only one subscription.");
            return;
        }

        subscriber = s;

        subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {}

            @Override
            public void cancel() {
                subscriber = null;
            }
        });
    }

    public void next(T message) {
        if (subscriber != null) {
            subscriber.onNext(message);
        }
    }

    public void error(Throwable error) {
        if (subscriber != null) {
            subscriber.onError(error);
        }
    }

    public void done() {
        if (subscriber != null) {
           subscriber.onComplete();
        }
    }
}
