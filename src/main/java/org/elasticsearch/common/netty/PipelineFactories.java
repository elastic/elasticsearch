package org.elasticsearch.common.netty;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.transport.netty.NettyTransport;
import org.elasticsearch.transport.netty.MessageChannelHandler;
import org.elasticsearch.common.netty.OpenChannelsHandler;
import org.elasticsearch.common.inject.Inject;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;

public class PipelineFactories {

    @Inject()
    public PipelineFactories() {
    }

    public ChannelPipelineFactory serverPipelineFactory(final NettyTransport transport, final OpenChannelsHandler serverOpenChannels, final ESLogger logger) throws ElasticSearchException {
        return new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("openChannels", serverOpenChannels);
                pipeline.addLast("dispatcher", new MessageChannelHandler(transport, logger));
                return pipeline;
            }
        };
    }

    public ChannelPipelineFactory clientPipelineFactory(final NettyTransport transport, final ESLogger logger) throws ElasticSearchException {
        return new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("dispatcher", new MessageChannelHandler(transport, logger));
                return pipeline;
            }
        };
    }
}