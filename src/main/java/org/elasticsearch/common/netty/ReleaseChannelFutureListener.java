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

package org.elasticsearch.common.netty;

import org.elasticsearch.common.lease.Releasable;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

/**
 * A channel listener that releases a {@link org.elasticsearch.common.lease.Releasable} when
 * the operation is complete.
 */
public class ReleaseChannelFutureListener implements ChannelFutureListener {

    private final Releasable releasable;

    public ReleaseChannelFutureListener(Releasable releasable) {
        this.releasable = releasable;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
        releasable.close();
    }
}
