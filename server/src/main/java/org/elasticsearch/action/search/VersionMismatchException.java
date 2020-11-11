package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;

public class VersionMismatchException extends ElasticsearchException {

    public VersionMismatchException(String msg, Object... args) {
        super(msg, args);
    }

}
