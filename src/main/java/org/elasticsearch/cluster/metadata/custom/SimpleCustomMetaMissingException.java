package org.elasticsearch.cluster.metadata.custom;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.rest.RestStatus;

public class SimpleCustomMetaMissingException extends ElasticSearchException {

    private final String name;

    public SimpleCustomMetaMissingException(String name) {
        super("index_custom_meta [" + name + "] missing");
        this.name = name;
    }

    public String name() {
        return this.name;
    }


    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }
}