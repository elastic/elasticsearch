package org.elasticsearch.example.module2;

import org.elasticsearch.example.module1.api.Module1Listener;
import org.elasticsearch.example.module1.api.Module1Service;
import org.elasticsearch.example.module2.api.Module2Service;
import org.elasticsearch.nalbind.api.Inject;

public class Module2ServiceImpl implements Module2Service, Module1Listener {
    private final Module1Service module1Service;

    public Module2ServiceImpl() {
        throw new UnsupportedOperationException();
    }

    @Inject
    public Module2ServiceImpl(Module1Service module1Service) {
        this.module1Service = module1Service;
    }

    @Override
    public String statusReport() {
        return "Module1Service: " + module1Service.greeting();
    }

    @Override
    public void yo() {
        System.out.println("Yo!");
    }
}
