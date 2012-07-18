package org.elasticsearch.transport.netty.ssl;

import javax.net.ssl.SSLEngine;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.transport.netty.SizeHeaderFrameDecoder;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.ssl.SslHandler;

/**
 * ChannelPipelineFactory used for Client SSL channel pipelines
 * 
 * @author Tanguy Leroux
 *
 */
public class SecureClientChannelPipelineFactory extends SSLChannelPipelineFactory {

    public SecureClientChannelPipelineFactory(SecureMessageChannelHandler channelHandler,
    		String sslKeyStore, String sslKeyStorePassword, String sslKeyStoreAlgorithm,
			String sslTrustStore, String sslTrustStorePassword, String sslTrustStoreAlgorithm,
			ByteSizeValue maxCumulationBufferCapacity, int maxCompositeBufferComponents) {
		super(channelHandler, 
				sslKeyStore, sslKeyStorePassword, sslKeyStoreAlgorithm,
				sslTrustStore, sslTrustStorePassword, sslTrustStoreAlgorithm,
				maxCumulationBufferCapacity, maxCompositeBufferComponents);
	}

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        SSLEngine engine = getSslContext().createSSLEngine();
        engine.setUseClientMode(true);

        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("ssl", new SslHandler(engine));
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
