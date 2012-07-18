package org.elasticsearch.transport.netty.ssl;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.transport.SSLTransportException;
import org.elasticsearch.transport.netty.MessageChannelHandler;
import org.elasticsearch.transport.netty.NettyTransport;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.handler.ssl.SslHandler;

/**
 * SSL message channel handler
 * 
 * @author Tanguy Leroux
 *
 */
public class SecureMessageChannelHandler extends MessageChannelHandler {

	private static final ESLogger logger = Loggers.getLogger(SecureMessageChannelHandler.class);
	
    public SecureMessageChannelHandler(NettyTransport transport, ESLogger logger) {
        super(transport, logger);
    }

    @Override
    public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
        SslHandler sslHandler = ctx.getPipeline().get(SslHandler.class);
        sslHandler.handshake();

        // Get notified when SSL handshake is done.
        final ChannelFuture handshakeFuture = sslHandler.handshake();
        handshakeFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                	logger.debug("SSL / TLS handshake completed for the channel.");
                    ctx.sendUpstream(e);
                } else {
                	logger.error("SSL / TLS handshake failed, closing the channel");
                    future.getChannel().close();
                    throw new SSLTransportException("SSL / TLS handshake failed, closing the channel", future.getCause());
                }
            }
        });
    }
}
