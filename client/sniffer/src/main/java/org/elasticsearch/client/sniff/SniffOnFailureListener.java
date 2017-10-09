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

package org.elasticsearch.client.sniff;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link org.elasticsearch.client.RestClient.FailureListener} implementation that allows to perform
 * sniffing on failure. Gets notified whenever a failure happens and uses a {@link Sniffer} instance
 * to manually reload hosts and sets them back to the {@link RestClient}. The {@link Sniffer} instance
 * needs to be lazily set through {@link #setSniffer(Sniffer)}.
 */
public class SniffOnFailureListener extends RestClient.FailureListener {

    private volatile Sniffer sniffer;
    private final AtomicBoolean set;

    public SniffOnFailureListener() {
        this.set = new AtomicBoolean(false);
    }

    /**
     * Sets the {@link Sniffer} instance used to perform sniffing
     * @throws IllegalStateException if the sniffer was already set, as it can only be set once
     */
    public void setSniffer(Sniffer sniffer) {
        Objects.requireNonNull(sniffer, "sniffer must not be null");
        if (set.compareAndSet(false, true)) {
            this.sniffer = sniffer;
        } else {
            throw new IllegalStateException("sniffer can only be set once");
        }
    }

    @Override
    public void onFailure(HttpHost host) {
        if (sniffer == null) {
            throw new IllegalStateException("sniffer was not set, unable to sniff on failure");
        }
        //re-sniff immediately but take out the node that failed
        sniffer.sniffOnFailure(host);
    }
}
