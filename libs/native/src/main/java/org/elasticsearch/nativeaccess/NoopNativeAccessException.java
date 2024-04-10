package org.elasticsearch.nativeaccess;

import org.elasticsearch.ElasticsearchException;

public class NoopNativeAccessException extends ElasticsearchException {
    public NoopNativeAccessException(String msg) {
        super(msg);
    }
}
