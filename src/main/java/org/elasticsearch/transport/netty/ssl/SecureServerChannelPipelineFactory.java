package org.elasticsearch.transport.netty.ssl;

import javax.net.ssl.SSLEngine;

import org.elasticsearch.common.netty.OpenChannelsHandler;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.transport.netty.SizeHeaderFrameDecoder;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.ssl.SslHandler;

/**
 * ChannelPipelineFactory used for Server SSL channel pipelines
 * 
 * @author Tanguy Leroux
 *
 */
public class SecureServerChannelPipelineFactory extends SSLChannelPipelineFactory {
	
	private OpenChannelsHandler serverOpenChannels;
    
    public SecureServerChannelPipelineFactory(SecureMessageChannelHandler channelHandler, OpenChannelsHandler openChannels,
			String sslKeyStore, String sslKeyStorePassword, String sslKeyStoreAlgorithm,
			String sslTrustStore, String sslTrustStorePassword, String sslTrustStoreAlgorithm,
			ByteSizeValue maxCumulationBufferCapacity, int maxCompositeBufferComponents) {
    	
    	super(channelHandler, sslKeyStore, sslKeyStorePassword, sslKeyStoreAlgorithm,
				sslTrustStore, sslTrustStorePassword, sslTrustStoreAlgorithm,
				maxCumulationBufferCapacity, maxCompositeBufferComponents);
    	this.serverOpenChannels = openChannels;
	}

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        SSLEngine engine = getSslContext().createSSLEngine();
        engine.setUseClientMode(false);
        engine.setNeedClientAuth(true);

        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("ssl", new SslHandler(engine));
        pipeline.addLast("openChannels", serverOpenChannels);
        SizeHeaderFrameDecoder sizeHeader = new SizeHeaderFrameDecoder();
        if (maxCumulationBufferCapacity != null) {
            if (maxCumulationBufferCapacity.bytes() > Integer.MAX_VALUE) {
                sizeHeader.setMaxCumulationBufferCapacity(Integer.MAX_VALUE);
            } else {
                sizeHeader.setMaxCumulationBufferCapacity((int) maxCumulationBufferCapacity.bytes());
            }
        }
        if (maxCompositeBufferComponents != -1) {
            sizeHeader.setMaxCumulationBufferComponents(maxCompositeBufferComponents);
        }
        pipeline.addLast("size", sizeHeader);
        pipeline.addLast("dispatcher", messageChannelHandler);
        return pipeline;
        
    }
}
