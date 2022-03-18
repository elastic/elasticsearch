/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.nio;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.concurrent.FutureUtils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * This is an {@link BiConsumer} that interfaces with netty code. It wraps a netty promise and will
 * complete that promise when accept is called. It delegates the normal promise methods to the underlying
 * promise.
 */
public class NettyListener implements BiConsumer<Void, Exception>, ChannelPromise {

    private final ChannelPromise promise;

    private NettyListener(ChannelPromise promise) {
        this.promise = promise;
    }

    @Override
    public void accept(Void v, Exception exception) {
        if (exception == null) {
            promise.setSuccess();
        } else {
            promise.setFailure(exception);
        }
    }

    @Override
    public Channel channel() {
        return promise.channel();
    }

    @Override
    public ChannelPromise setSuccess(Void result) {
        return promise.setSuccess(result);
    }

    @Override
    public boolean trySuccess(Void result) {
        return promise.trySuccess(result);
    }

    @Override
    public ChannelPromise setSuccess() {
        return promise.setSuccess();
    }

    @Override
    public boolean trySuccess() {
        return promise.trySuccess();
    }

    @Override
    public ChannelPromise setFailure(Throwable cause) {
        return promise.setFailure(cause);
    }

    @Override
    public boolean tryFailure(Throwable cause) {
        return promise.tryFailure(cause);
    }

    @Override
    public boolean setUncancellable() {
        return promise.setUncancellable();
    }

    @Override
    public boolean isSuccess() {
        return promise.isSuccess();
    }

    @Override
    public boolean isCancellable() {
        return promise.isCancellable();
    }

    @Override
    public Throwable cause() {
        return promise.cause();
    }

    @Override
    public ChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
        return promise.addListener(listener);
    }

    @Override
    @SafeVarargs
    @SuppressWarnings("varargs")
    public final ChannelPromise addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        return promise.addListeners(listeners);
    }

    @Override
    public ChannelPromise removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
        return promise.removeListener(listener);
    }

    @Override
    @SafeVarargs
    @SuppressWarnings("varargs")
    public final ChannelPromise removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        return promise.removeListeners(listeners);
    }

    @Override
    public ChannelPromise sync() throws InterruptedException {
        return promise.sync();
    }

    @Override
    public ChannelPromise syncUninterruptibly() {
        return promise.syncUninterruptibly();
    }

    @Override
    public ChannelPromise await() throws InterruptedException {
        return promise.await();
    }

    @Override
    public ChannelPromise awaitUninterruptibly() {
        return promise.awaitUninterruptibly();
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return promise.await(timeout, unit);
    }

    @Override
    public boolean await(long timeoutMillis) throws InterruptedException {
        return promise.await(timeoutMillis);
    }

    @Override
    public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
        return promise.awaitUninterruptibly(timeout, unit);
    }

    @Override
    public boolean awaitUninterruptibly(long timeoutMillis) {
        return promise.awaitUninterruptibly(timeoutMillis);
    }

    @Override
    public Void getNow() {
        return promise.getNow();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return FutureUtils.cancel(promise);
    }

    @Override
    public boolean isCancelled() {
        return promise.isCancelled();
    }

    @Override
    public boolean isDone() {
        return promise.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        return promise.get();
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return promise.get(timeout, unit);
    }

    @Override
    public boolean isVoid() {
        return promise.isVoid();
    }

    @Override
    public ChannelPromise unvoid() {
        return promise.unvoid();
    }

    public static NettyListener fromBiConsumer(BiConsumer<Void, Exception> biConsumer, Channel channel) {
        if (biConsumer instanceof NettyListener nettyListener) {
            return nettyListener;
        } else {
            ChannelPromise channelPromise = channel.newPromise();
            channelPromise.addListener(f -> {
                Throwable cause = f.cause();
                if (cause == null) {
                    biConsumer.accept(null, null);
                } else {
                    if (cause instanceof Exception exception) {
                        biConsumer.accept(null, exception);
                    } else {
                        ExceptionsHelper.maybeDieOnAnotherThread(cause);
                        biConsumer.accept(null, new Exception(cause));
                    }
                }
            });

            return new NettyListener(channelPromise);
        }
    }

    public static NettyListener fromChannelPromise(ChannelPromise channelPromise) {
        if (channelPromise instanceof NettyListener nettyListener) {
            return nettyListener;
        } else {
            return new NettyListener(channelPromise);
        }
    }
}
