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
