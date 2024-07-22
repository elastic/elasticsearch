package org.elasticsearch.example.module1;

import org.elasticsearch.example.module1.api.Module1Listener;
import org.elasticsearch.example.module1.api.Module1Service;
import org.elasticsearch.nalbind.api.Inject;

import java.util.List;

public class Module1ServiceImpl implements Module1Service {
    final List<Module1Listener> listeners;

    @Inject
    public Module1ServiceImpl(List<Module1Listener> listeners) {
        this.listeners = listeners;
    }

    @Override
    public String greeting() {
        return "Hello from " + getClass().getSimpleName() + " to my " + listeners.size() + " listeners";
    }
}
