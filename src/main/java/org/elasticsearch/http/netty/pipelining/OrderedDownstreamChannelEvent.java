package org.elasticsearch.http.netty.pipelining;

import org.jboss.netty.channel.*;

/**
 * Permits downstream channel events to be ordered and signalled as to whether more are to come for a given sequence.
 *
 * @author Christopher Hunt
 */
public class OrderedDownstreamChannelEvent implements ChannelEvent {

    final ChannelEvent ce;
    final OrderedUpstreamMessageEvent oue;
    final int subsequence;
    final boolean last;

    /**
     * Construct a downstream channel event for all types of events.
     *
     * @param oue         the OrderedUpstreamMessageEvent that this response is associated with
     * @param subsequence the sequence within the sequence
     * @param last        when set to true this indicates that there are no more responses to be received for the
     *                    original OrderedUpstreamMessageEvent
     */
    public OrderedDownstreamChannelEvent(final OrderedUpstreamMessageEvent oue, final int subsequence, boolean last,
                                         final ChannelEvent ce) {
        this.oue = oue;
        this.ce = ce;
        this.subsequence = subsequence;
        this.last = last;
    }

    /**
     * Convenience constructor signifying that this downstream message event is the last one for the given sequence,
     * and that there is only one response.
     */
    public OrderedDownstreamChannelEvent(final OrderedUpstreamMessageEvent oe,
                                         final Object message) {
        this(oe, 0, true, message);
    }

    /**
     * Convenience constructor for passing message events.
     */
    public OrderedDownstreamChannelEvent(final OrderedUpstreamMessageEvent oue, final int subsequence, boolean last,
                                         final Object message) {
        this(oue, subsequence, last, new DownstreamMessageEvent(oue.getChannel(), Channels.future(oue.getChannel()),
                message, oue.getRemoteAddress()));

    }

    public OrderedUpstreamMessageEvent getOrderedUpstreamMessageEvent() {
        return oue;
    }

    public int getSubsequence() {
        return subsequence;
    }

    public boolean isLast() {
        return last;
    }

    @Override
    public Channel getChannel() {
        return ce.getChannel();
    }

    @Override
    public ChannelFuture getFuture() {
        return ce.getFuture();
    }

    public ChannelEvent getChannelEvent() {
        return ce;
    }
}
