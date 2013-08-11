package org.elasticsearch.action.percolate;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class TransportMultiPercolateAction extends TransportAction<MultiPercolateRequest, MultiPercolateResponse> {

    private final TransportPercolateAction percolateAction;
    private final ClusterService clusterService;

    @Inject
    public TransportMultiPercolateAction(Settings settings, ThreadPool threadPool, TransportPercolateAction percolateAction, ClusterService clusterService, TransportService transportService) {
        super(settings, threadPool);
        this.percolateAction = percolateAction;
        this.clusterService = clusterService;

        transportService.registerHandler(MultiPercolateAction.NAME, new TransportHandler());
    }

    @Override
    protected void doExecute(MultiPercolateRequest request, final ActionListener<MultiPercolateResponse> listener) {
        ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final MultiPercolateResponse.Item[] responses = new MultiPercolateResponse.Item[request.requests().size()];
        final AtomicInteger counter = new AtomicInteger(responses.length);
        for (int i = 0; i < responses.length; i++) {
            final int index = i;
            percolateAction.execute(request.requests().get(i), new ActionListener<PercolateResponse>() {
                @Override
                public void onResponse(PercolateResponse percolateResponse) {
                    synchronized (responses) {
                        responses[index] = new MultiPercolateResponse.Item(percolateResponse, null);
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    synchronized (responses) {
                        responses[index] = new MultiPercolateResponse.Item(null, ExceptionsHelper.detailedMessage(e));
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    listener.onResponse(new MultiPercolateResponse(responses));
                }
            });
        }
    }

    class TransportHandler extends BaseTransportRequestHandler<MultiPercolateRequest> {

        @Override
        public MultiPercolateRequest newInstance() {
            return new MultiPercolateRequest();
        }

        @Override
        public void messageReceived(final MultiPercolateRequest request, final TransportChannel channel) throws Exception {
            // no need to use threaded listener, since we just send a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<MultiPercolateResponse>() {
                @Override
                public void onResponse(MultiPercolateResponse response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send error response for action [msearch] and request [" + request + "]", e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

}
