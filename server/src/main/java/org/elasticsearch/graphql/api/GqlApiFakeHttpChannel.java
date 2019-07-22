package org.elasticsearch.graphql.api;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpResponse;

import java.net.InetSocketAddress;

public class GqlApiFakeHttpChannel implements HttpChannel {

    private final InetSocketAddress remoteAddress;

    public GqlApiFakeHttpChannel(InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    @Override
    public void sendResponse(HttpResponse response, ActionListener<Void> listener) {

    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return null;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {

    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() {

    }
}
