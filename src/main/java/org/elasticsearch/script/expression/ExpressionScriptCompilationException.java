package org.elasticsearch.script.expression;

import org.elasticsearch.ElasticsearchException;

import java.text.ParseException;

/**
 * Exception representing a compilation error in an expression.
 */
public class ExpressionScriptCompilationException extends ElasticsearchException {
    public ExpressionScriptCompilationException(String msg, ParseException e) {
        super(msg, e);
    }
}
