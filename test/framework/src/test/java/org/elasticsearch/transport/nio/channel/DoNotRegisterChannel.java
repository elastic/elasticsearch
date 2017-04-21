package org.elasticsearch.transport.nio.channel;

import org.elasticsearch.transport.nio.ESSelector;
import org.elasticsearch.transport.nio.utils.TestSelectionKey;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

public class DoNotRegisterChannel extends NioSocketChannel {

    public DoNotRegisterChannel(String profile, SocketChannel socketChannel) throws IOException {
        super(profile, socketChannel);
    }

    @Override
    public boolean register(ESSelector selector) throws ClosedChannelException {
        if (markRegistered(selector)) {
            setSelectionKey(new TestSelectionKey(0));
            return true;
        } else {
            return false;
        }
    }
}
