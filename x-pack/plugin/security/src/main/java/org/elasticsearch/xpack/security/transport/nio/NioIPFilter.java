/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import java.util.function.Predicate;

public final class NioIPFilter implements Predicate<NioSocketChannel> {

    private final IPFilter filter;
    private final String profile;

    NioIPFilter(@Nullable IPFilter filter, String profile) {
        this.filter = filter;
        this.profile = profile;
    }

    @Override
    public boolean test(NioSocketChannel nioChannel) {
        if (filter != null) {
            return filter.accept(profile, nioChannel.getRemoteAddress());
        } else {
            return true;
        }
    }
}
