/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Objects;

/**
 * Parses the JSON output of a process.
 * <p>
 * Expects an array of objects so the first element will always be the
 * start array symbol and the data must be terminated with the end array symbol.
 */
public class ProcessResultsParser<T> {

    private static final Logger logger = LogManager.getLogger(ProcessResultsParser.class);

    private final ConstructingObjectParser<T, Void> resultParser;
    private final NamedXContentRegistry namedXContentRegistry;

    public ProcessResultsParser(ConstructingObjectParser<T, Void> resultParser, NamedXContentRegistry namedXContentRegistry) {
        this.resultParser = Objects.requireNonNull(resultParser);
        this.namedXContentRegistry = Objects.requireNonNull(namedXContentRegistry);
    }

    public Iterator<T> parseResults(InputStream in) throws ElasticsearchParseException {
        try {
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, in);
            XContentParser.Token token = parser.nextToken();
            // if start of an array ignore it, we expect an array of results
            if (token != XContentParser.Token.START_ARRAY) {
                throw new ElasticsearchParseException("unexpected token [" + token + "]");
            }
            return new ResultIterator(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException(e.getMessage(), e);
        }
    }

    private class ResultIterator implements Iterator<T> {

        private final XContentParser parser;
        private XContentParser.Token token;

        private ResultIterator(XContentParser parser) {
            this.parser = parser;
            token = parser.currentToken();
        }

        @Override
        public boolean hasNext() {
            try {
                token = parser.nextToken();
            } catch (IOException e) {
                logger.debug("io error while parsing", e);
                return false;
            }
            if (token == XContentParser.Token.END_ARRAY) {
                return false;
            } else if (token != XContentParser.Token.START_OBJECT) {
                logger.error("Expecting Json Field name token after the Start Object token");
                throw new ElasticsearchParseException("unexpected token [" + token + "]");
            }
            return true;
        }

        @Override
        public T next() {
            return resultParser.apply(parser, null);
        }
    }
}
