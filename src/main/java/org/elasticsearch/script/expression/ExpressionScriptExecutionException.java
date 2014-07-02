package org.elasticsearch.script.expression;

import org.elasticsearch.ElasticsearchException;

/**
 * Exception used to wrap exceptions occuring while running expressions.
 */
public class ExpressionScriptExecutionException extends ElasticsearchException {
    public ExpressionScriptExecutionException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
