/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.http.HttpContent;

import java.util.concurrent.Flow;

public class Netty4RequestContentPublisher implements Flow.Publisher<HttpContent> {

    private final Channel channel;
    private long requested = 0;
    private Flow.Subscriber<? super HttpContent> subscriber;

    public Netty4RequestContentPublisher(Channel channel) {
        this.channel = channel;
        channel.config().setAutoRead(false);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super HttpContent> subscriber) {
        this.subscriber = subscriber;
        subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
                requested += n;
                if (requested > 0) {
                    channel.read();
                }
            }

            @Override
            public void cancel() {}
        });
    }

    public void sendChunk(io.netty.handler.codec.http.HttpContent chunk) {
        assert subscriber != null;
        if (chunk != LastHttpContent.EMPTY_LAST_CONTENT) {
            var content = new Netty4HttpContent(chunk);
            subscriber.onNext(content);
            requested -= 1;
            if (requested > 0) channel.read();
        }
        if (chunk instanceof LastHttpContent) {
            subscriber.onComplete();
            channel.config().setAutoRead(true);
        }
    }

}
