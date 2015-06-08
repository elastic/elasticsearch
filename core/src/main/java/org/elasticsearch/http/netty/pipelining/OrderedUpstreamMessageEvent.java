package org.elasticsearch.http.netty.pipelining;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.UpstreamMessageEvent;

import java.net.SocketAddress;

/**
 * Permits upstream message events to be ordered.
 *
 * @author Christopher Hunt
 */
public class OrderedUpstreamMessageEvent extends UpstreamMessageEvent {
    final int sequence;

    public OrderedUpstreamMessageEvent(final int sequence, final Channel channel, final Object msg, final SocketAddress remoteAddress) {
        super(channel, msg, remoteAddress);
        this.sequence = sequence;
    }

    public int getSequence() {
        return sequence;
    }

}
